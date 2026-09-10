package api

import (
	"errors"
	"testing"

	"github.com/72olabs/holler/internal/bus"
)

func TestAuthorizeHostPeerRequiresExactDirectParentProcess(t *testing.T) {
	harness := HarnessProcessIdentity{
		Handle:  "hin_test",
		Harness: ProcessIdentity{PID: 67533, StartFingerprint: "claude-start"},
		Host:    ProcessIdentity{PID: 8784, StartFingerprint: "t3-server-start"},
	}
	if err := authorizeHostPeer(harness, ProcessIdentity{PID: 8784, StartFingerprint: "t3-server-start"}); err != nil {
		t.Fatalf("exact direct parent rejected: %v", err)
	}

	for _, test := range []struct {
		name string
		peer ProcessIdentity
	}{
		{name: "claude", peer: ProcessIdentity{PID: 67533, StartFingerprint: "claude-start"}},
		{name: "model descendant", peer: ProcessIdentity{PID: 68000, StartFingerprint: "bash-start"}},
		{name: "sibling waiter", peer: ProcessIdentity{PID: 8790, StartFingerprint: "waiter-start"}},
		{name: "neighboring agent descendant", peer: ProcessIdentity{PID: 34363, StartFingerprint: "codex-start"}},
		{name: "grandparent", peer: ProcessIdentity{PID: 8754, StartFingerprint: "t3-app-start"}},
		{name: "reused parent pid", peer: ProcessIdentity{PID: 8784, StartFingerprint: "different-start"}},
		{name: "missing peer start", peer: ProcessIdentity{PID: 8784}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := authorizeHostPeer(harness, test.peer)
			var validation *bus.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("error = %v, want validation error", err)
			}
		})
	}
}

func TestAuthorizeHostPeerRejectsIneligibleRecordedParent(t *testing.T) {
	for _, host := range []ProcessIdentity{
		{PID: 1, StartFingerprint: "launchd-start"},
		{PID: 0, StartFingerprint: "missing-pid"},
		{PID: 8784},
	} {
		harness := HarnessProcessIdentity{Handle: "hin_test", Host: host}
		err := authorizeHostPeer(harness, host)
		var validation *bus.ValidationError
		if !errors.As(err, &validation) {
			t.Fatalf("host %+v error = %v, want validation error", host, err)
		}
	}
}

func TestHostAttentionCapabilityIsExperimental(t *testing.T) {
	standard := NewServer(nil)
	if containsString(standard.protocolCapabilities(), HostAttentionCapability) {
		t.Fatal("standard server advertised experimental host attention")
	}
	experimental := NewServer(nil, WithExperimentalHostAttention(true))
	if !containsString(experimental.protocolCapabilities(), HostAttentionCapability) {
		t.Fatal("experimental server omitted host attention capability")
	}
}
