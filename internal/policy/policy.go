// Package policy implements Intercom's operation-scoped authorization
// decisions with deny-overrides, explainable outcomes.
//
// The package is intentionally free of store/transport dependencies: the
// node supplies facts (same principal, grant role, grant active, delivery
// policy, adapter capability) and policy decides. This keeps the decision
// logic testable deterministically and prevents duplicate competing
// implementations in the node.
package policy

import "github.com/B-A-M-N/intercom/internal/protocol"

// Operation names the action being authorized.
type Operation string

const (
	OpDiscover       Operation = "discover"
	OpViewCaps       Operation = "view_capabilities"
	OpSendNotify     Operation = "send_notify"
	OpSendQueue      Operation = "send_queue"
	OpSendSteer      Operation = "send_steer"
	OpWake           Operation = "wake"
	OpSubscribe      Operation = "subscribe"
	OpHistory        Operation = "history"
	OpRelay          Operation = "relay"
	OpManagePolicy   Operation = "manage_policy"
	OpApprove        Operation = "approve"
)

// Facts are the authorization inputs for one decision.
type Facts struct {
	SenderPrincipal string
	SenderSession   string
	TargetPrincipal string
	TargetSession   string
	SamePrincipal   bool
	Self            bool
	GrantActive     bool // peer or owner grant, unexpired and unrevoked
	GrantIsOwner    bool
	GrantRevoked    bool // explicit deny: revoked/expired grant exists
	DeliveryAllowed bool // recipient delivery policy allows the mode (or no policy row for notify/queue)
	DeliveryPolicySet bool
	CapabilityNative bool // recipient adapter natively supports steer (for steer only)
}

// Decision is an explainable authorization outcome.
type Decision struct {
	Allowed bool
	Rule    string
	Reason  string
}

// Evaluate applies deny-overrides: any explicit deny wins over any allow.
func Evaluate(op Operation, f Facts) Decision {
	// Explicit denies first.
	if op == OpApprove {
		if !f.Self && !f.GrantIsOwner {
			return Decision{false, "approve.owner-only", "only the session itself or an owner-role grant may approve"}
		}
		return Decision{true, "approve.owner", "owner authority for approve"}
	}
	if op == OpManagePolicy {
		if !f.Self && !f.GrantIsOwner {
			return Decision{false, "manage.owner-only", "only the session itself or an owner-role grant may manage policy"}
		}
		return Decision{true, "manage.owner", "owner authority for policy management"}
	}
	// Delivery policy deny overrides same-principal and grant allows.
	if op == OpSendNotify || op == OpSendQueue || op == OpSendSteer || op == OpWake {
		if f.DeliveryPolicySet && !f.DeliveryAllowed {
			return Decision{false, "delivery-policy.deny", "recipient delivery policy excludes this mode"}
		}
	}
	// Explicit revocation is authoritative (deny-overrides): a revoked or
	// expired grant denies even same-principal peers. Only an absent grant
	// (never issued) leaves same-principal communication to the default.
	if f.GrantRevoked && !f.Self {
		return Decision{false, "grant.revoked", "grant revoked or expired"}
	}
	switch op {
	case OpDiscover, OpViewCaps, OpSubscribe, OpHistory:
		if f.Self || f.SamePrincipal || f.GrantActive {
			return Decision{true, "visible", "same principal, self, or active grant"}
		}
		return Decision{false, "visible.deny", "cross-principal without an active grant"}
	case OpSendNotify, OpSendQueue:
		if f.Self || f.SamePrincipal || f.GrantActive {
			return Decision{true, "send.bounded", "same principal or active grant, recipient policy allows"}
		}
		return Decision{false, "send.deny", "cross-principal communication requires an active grant"}
	case OpSendSteer:
		// Authorization for steering: same-principal (trusted local) or
		// active grant, recipient delivery policy not denying. Native
		// capability is enforced at delivery time by the capability
		// matrix (reject/downgrade/notify per OnUnsupported), so a
		// steer request can be admitted and then explicitly rejected
		// or downgraded rather than silently simulated.
		if f.Self || f.SamePrincipal || f.GrantActive {
			return Decision{true, "steer.allowed", "recipient policy allows, same principal or grant; capability enforced at delivery"}
		}
		return Decision{false, "steer.deny", "cross-principal steering requires an active grant"}
	case OpWake:
		if f.Self || f.SamePrincipal || f.GrantActive {
			return Decision{true, "wake.allowed", "same principal or active grant"}
		}
		return Decision{false, "wake.deny", "cross-principal wake requires an active grant"}
	case OpRelay:
		if f.GrantActive || f.SamePrincipal {
			return Decision{true, "relay.allowed", "relay requires same principal or active grant and hop limit (enforced in routing)"}
		}
		return Decision{false, "relay.deny", "relay requires an active grant"}
	default:
		return Decision{false, "unknown-op", "unknown operation denied by default"}
	}
}

// OperationForMode maps a delivery mode to its send operation.
func OperationForMode(m protocol.DeliveryMode) Operation {
	switch m {
	case protocol.DeliveryNotify:
		return OpSendNotify
	case protocol.DeliveryQueue:
		return OpSendQueue
	case protocol.DeliverySteer:
		return OpSendSteer
	default:
		return OpSendQueue
	}
}
