package protocol

// Qualification levels separate what is announced from what is actually
// available, per ICSP-001 sections 1.1, 8.3, and 12.
//
//   - Advertised:    the adapter's capability table declares the feature.
//   - Implemented:   real harness code executes it (not a simulation).
//   - Tested:        automated tests exercise the real code path.
//   - LiveVerified:  observed working against the real harness binary
//     in its documented, current form.
//
// A capability is only "available" when Advertised AND Implemented.
// Tested and LiveVerified describe evidence, never capability alone:
// a Codex steer verified in one deployment does not make another
// harness able to steer.

// CapabilityQualification records the four-level qualification state of
// one capability on one harness.
type CapabilityQualification struct {
	Harness      string       `json:"harness"`
	Capability   CapabilityID `json:"capability"`
	Advertised   bool         `json:"advertised"`
	Implemented  bool         `json:"implemented"`
	Tested       bool         `json:"tested"`
	LiveVerified bool         `json:"liveVerified"`
	Notes        string       `json:"notes,omitempty"`
}

// Available reports whether the capability may be used on this harness.
func (q CapabilityQualification) Available() bool {
	return q.Advertised && q.Implemented
}

// HarnessQualificationMatrix returns the qualification table for every
// known adapter. Unknown harnesses have no row: absence means nothing
// is claimed. Generic/synthetic rows are omitted because their delivery
// modes are declared per-instance at registration time.
func HarnessQualificationMatrix() []CapabilityQualification {
	muse := []CapabilityQualification{
		{Harness: "muse", Capability: CapDeliveryNotify, Advertised: true, Implemented: true, Tested: true, Notes: "operator surface only; never injected"},
		{Harness: "muse", Capability: CapDeliveryQueue, Advertised: true, Implemented: true, Tested: true},
		{Harness: "muse", Capability: CapDeliverySteer, Advertised: true, Implemented: true, Tested: true, LiveVerified: true, Notes: "MSP turn/steer; see docs/HARNESSES.md"},
	}
	codex := []CapabilityQualification{
		{Harness: "codex", Capability: CapDeliveryNotify, Advertised: true, Implemented: true, Tested: true},
		{Harness: "codex", Capability: CapDeliveryQueue, Advertised: true, Implemented: true, Tested: true},
		{Harness: "codex", Capability: CapDeliverySteer, Advertised: true, Implemented: true, Tested: true, LiveVerified: true, Notes: "app-server turn/steer; see docs/HARNESSES.md"},
	}
	unsupportedSteer := func(h string) []CapabilityQualification {
		return []CapabilityQualification{
			{Harness: h, Capability: CapDeliveryNotify, Advertised: true, Implemented: true, Tested: true},
			{Harness: h, Capability: CapDeliveryQueue, Advertised: true, Implemented: true, Tested: true},
			{Harness: h, Capability: CapDeliverySteer, Advertised: false, Implemented: false, Tested: true, Notes: "no native active-turn injection; claiming it would misrepresent queue/prompt as steer"},
		}
	}
	out := make([]CapabilityQualification, 0, 15)
	out = append(out, muse...)
	out = append(out, codex...)
	out = append(out, unsupportedSteer("opencode")...)
	out = append(out, unsupportedSteer("hermes")...)
	out = append(out, unsupportedSteer(HarnessClaude)...)
	return out
}

// QualificationsFor returns the qualification rows for one harness,
// or nil when the harness is unknown.
func QualificationsFor(harness string) []CapabilityQualification {
	var out []CapabilityQualification
	for _, q := range HarnessQualificationMatrix() {
		if q.Harness == harness {
			out = append(out, q)
		}
	}
	return out
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