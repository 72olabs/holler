package bus_test

import (
	"encoding/json"
	"testing"

	"github.com/72olabs/holler/internal/bus"
)

func TestCapabilityContractsRequireTypedObjects(t *testing.T) {
	descriptor := bus.CapabilityDescriptor{
		Name: "example.read", Mode: bus.CapabilityRead, Description: "Read an example.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
	}
	if err := bus.ValidateCapabilityDescriptor(descriptor); err != nil {
		t.Fatalf("valid descriptor: %v", err)
	}
	for name, invocation := range map[string]bus.CapabilityInvocation{
		"missing name":  {Arguments: json.RawMessage(`{}`)},
		"null args":     {Name: "example.read", Arguments: json.RawMessage(`null`)},
		"array args":    {Name: "example.read", Arguments: json.RawMessage(`[]`)},
		"trailing JSON": {Name: "example.read", Arguments: json.RawMessage(`{} {}`)},
	} {
		t.Run(name, func(t *testing.T) {
			if err := bus.ValidateCapabilityInvocation(invocation); err == nil {
				t.Fatal("invalid invocation was accepted")
			}
		})
	}
}
