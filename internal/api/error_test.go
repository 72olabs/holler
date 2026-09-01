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
