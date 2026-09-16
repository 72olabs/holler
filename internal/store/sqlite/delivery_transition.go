package sqlite

import (
	"database/sql"
	"github.com/72olabs/holler/internal/bus"
)

// finishDeliveryState is shared by both delivery stores. A terminal retry is
// idempotent only for the same token and operation. Expired, unreclaimed tokens
// retain the legacy finish semantics; a replacement claim fences the old token.
func finishDeliveryState(state bus.DeliveryState, stored, terminal sql.NullString, token string, ack, final bool) (bool, error) {
	if state == bus.DeliveryAcked && ack && terminal.Valid && terminal.String == token {
		return true, nil
	}
	if state == bus.DeliveryDeadLettered && final && !ack && terminal.Valid && terminal.String == token {
		return true, nil
	}
	if terminal.Valid && terminal.String == token && state == bus.DeliveryQueued {
		return false, bus.ErrDeliveryTerminal
	}
	if state == bus.DeliveryAcked || state == bus.DeliveryDeadLettered {
		return false, bus.ErrDeliveryTerminal
	}
	if state != bus.DeliveryClaimed || !stored.Valid || stored.String != token {
		return false, bus.ErrLeaseTokenMismatch
	}
	return false, nil
}
