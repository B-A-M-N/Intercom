package node

import (
	"github.com/B-A-M-N/intercom/internal/protocol"
)

// Conformance profiles defined by ICSP-001 section 14.
//
// Profiles are cumulative only where their stated prerequisites apply, and an
// implementation MUST identify its supported profiles individually.
const (
	// ProfileLocal is local authenticated peer communication and delivery receipts.
	ProfileLocal = "IC-LOCAL"
	// ProfileDurable adds command deduplication, durable admission, event
	// replay, and crash recovery.
	ProfileDurable = "IC-DURABLE"
	// ProfileACP is ACP v1 extension compatibility and capability negotiation.
	ProfileACP = "IC-ACP"
	// ProfileRemote is authenticated cross-host delivery with independent
	// durable receipt tracking.
	ProfileRemote = "IC-REMOTE"
	// ProfileSteer is verified live-turn message injection for one or more
	// explicitly identified harnesses.
	ProfileSteer = "IC-STEER"
)

// SupportedProfiles reports the conformance profiles this node implements.
//
// The local profile is implemented by the durable store and local transport.
// IC-REMOTE is not claimed: this build has no remote transport.
func (n *Node) SupportedProfiles() []string {
	return []string{ProfileLocal, ProfileDurable, ProfileACP}
}

// HarnessQualifications returns the per-harness qualification matrix:
// what each known adapter advertises, implements, tests, and has
// live-verified. Global profiles never imply per-harness capabilities.
func (n *Node) HarnessQualifications() []protocol.CapabilityQualification {
	return protocol.HarnessQualificationMatrix()
}

// CapabilityAdvertisement returns the capabilities this node advertises.
//
// Every capability states its support level, so a client can tell a natively
// implemented capability from an emulated one (ICSP-001 section 12).
func (n *Node) CapabilityAdvertisement() []protocol.Capability {
	return []protocol.Capability{
		{ID: protocol.CapPeerDiscovery, Support: protocol.SupportNative},
		{ID: protocol.CapPeerSend, Support: protocol.SupportNative},
		{ID: protocol.CapPeerReply, Support: protocol.SupportNative},
		{ID: protocol.CapDeliveryNotify, Support: protocol.SupportNative},
		{ID: protocol.CapDeliveryQueue, Support: protocol.SupportNative},
		{ID: protocol.CapDeliverySteer, Support: protocol.SupportUnsupported,
			Limitations: []string{
				"active-turn steering requires a harness-specific native integration",
			}},
		{ID: protocol.CapDeliveryWake, Support: protocol.SupportEmulated,
			Limitations: []string{
				"wake requests are honoured only while the recipient adapter is connected",
			}},
		{ID: protocol.CapSessionObserve, Support: protocol.SupportNative},
		{ID: protocol.CapSessionReplay, Support: protocol.SupportNative},
		{ID: protocol.CapCommandDurable, Support: protocol.SupportNative},
		{ID: protocol.CapApprovalOwner, Support: protocol.SupportNative,
			Limitations: []string{
				"peer authority never includes approval authority",
			}},
	}
}

// Harness names are the adapter's registered harness strings. Claude Code
// registers as "claude-code", not "claude".
const (
	HarnessMuse     = "muse"
	HarnessCodex    = "codex"
	HarnessOpenCode = "opencode"
	HarnessHermes   = "hermes"
	HarnessClaude   = "claude-code"
)
