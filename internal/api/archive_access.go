package api

import (
	"context"
	"strings"

	"github.com/72olabs/holler/internal/bus"
)

// Archive previews contain inbox bodies, unlike ordinary actor discovery. Keep
// this check shared by the direct RPC and dynamic capability bridge. A denied
// request must not reveal whether the target actor exists.
func archivePreflightForIdentity(ctx context.Context, store Store, identity Identity, actor string, limit int) (bus.ActorArchivePreflight, error) {
	actor = strings.TrimSpace(actor)
	if identity.Actor != "operator" && (identity.Actor == "" || identity.Actor != actor) {
		return bus.ActorArchivePreflight{}, bus.ErrNotFound
	}
	return store.ArchivePreflight(ctx, actor, limit)
}
