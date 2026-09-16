package api

import (
	"context"
	"path/filepath"
	"testing"

	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestManagedConversationHelloCapabilityRequiresEnablement(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		var options []ServerOption
		if enabled {
			name = "enabled"
			options = append(options, WithConversations())
		}
		t.Run(name, func(t *testing.T) {
			db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "holler.sqlite3"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			server := NewServer(db, options...)
			// The hello response and status both use protocolCapabilities.
			if got := containsString(server.protocolCapabilities(), ManagedConversationsCapability); got != enabled {
				t.Fatalf("managed hello capability = %v, enabled = %v", got, enabled)
			}
		})
	}
}
