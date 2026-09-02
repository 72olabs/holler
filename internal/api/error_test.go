package api

import (
	"errors"
	"testing"

	"github.com/72olabs/holler/internal/bus"
)

func TestAttentionUnavailableRPCContractIsTerminal(t *testing.T) {
	wire := rpcError(bus.ErrAttentionUnavailable)
	if wire.Code != "attention_unavailable" || wire.Retryable {
		t.Fatalf("wire error = %+v", wire)
	}
	decoded := errorFromRPC(wire)
	if !errors.Is(decoded, bus.ErrAttentionUnavailable) || decoded.Error() != bus.ErrAttentionUnavailable.Error() {
		t.Fatalf("decoded error = %v", decoded)
	}
}

func TestAliasRPCErrorsRoundTrip(t *testing.T) {
	for _, test := range []struct {
		err  error
		code string
	}{
		{bus.ErrAliasConflict, "alias_conflict"},
		{bus.ErrAliasNotFound, "alias_not_found"},
		{bus.ErrAliasTargetUnknown, "alias_target_unknown"},
	} {
		wire := rpcError(test.err)
		if wire.Code != test.code || wire.Retryable {
			t.Fatalf("wire error for %v = %+v", test.err, wire)
		}
		if decoded := errorFromRPC(wire); !errors.Is(decoded, test.err) {
			t.Fatalf("decoded %q = %v", test.code, decoded)
		}
	}
}

func TestRPCErrorContextDoesNotDuplicateSentinelText(t *testing.T) {
	wire := &RPCError{
		Code:    "alias_conflict",
		Message: "aliasA: " + bus.ErrAliasConflict.Error(),
	}
	decoded := errorFromRPC(wire)
	if !errors.Is(decoded, bus.ErrAliasConflict) {
		t.Fatalf("decoded error = %v", decoded)
	}
	if got, want := decoded.Error(), wire.Message; got != want {
		t.Fatalf("decoded error = %q, want %q", got, want)
	}
}
