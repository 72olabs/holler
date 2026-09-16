package sqlite

import (
	"errors"
	"github.com/72olabs/holler/internal/bus"
	"sync"
	"testing"
	"time"
)

func TestManagedDeliveryLeaseConcurrencyAndRevocation(t *testing.T) {
	s, ctx := channelFixture(t)
	c, err := s.ChannelCreate(ctx, agent("a"), bus.ChannelCreate{ProjectID: "test", Kind: "named", Title: "delivery", Participants: []string{"a", "b"}, IdempotencyKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	m := postChannel(t, s, ctx, agent("a"), c, "message")
	inbox, err := s.ChannelInbox(ctx, agent("b"), 10)
	if err != nil || len(inbox) != 1 {
		t.Fatalf("inbox: %v %v", inbox, err)
	}
	claims := make(chan ChannelDelivery, 10)
	fail := make(chan error, 10)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claim, err := s.ChannelClaim(ctx, agent("b"), m.Message.ID, time.Minute)
			if err != nil {
				fail <- err
			} else {
				claims <- claim
			}
		}()
	}
	wg.Wait()
	close(claims)
	close(fail)
	if len(claims) != 1 {
		t.Fatalf("claims %d", len(claims))
	}
	claim := <-claims
	for err := range fail {
		if !errors.Is(err, bus.ErrNoMessage) {
			t.Fatal(err)
		}
	}
	if _, err := s.ChannelDeliveryUpdate(ctx, agent("b"), m.Message.ID, "wrong", "ack", "", 0); !errors.Is(err, bus.ErrLeaseTokenMismatch) {
		t.Fatal(err)
	}
	if _, err := s.ChannelDeliveryUpdate(ctx, agent("b"), m.Message.ID, claim.LeaseToken, "extend", "", time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelDeliveryUpdate(ctx, agent("b"), m.Message.ID, claim.LeaseToken, "nack", "retry", 0); err != nil {
		t.Fatal(err)
	}
	second, err := s.ChannelClaim(ctx, agent("b"), m.Message.ID, time.Minute)
	if err != nil || second.Attempt != 2 {
		t.Fatalf("reclaim: %+v %v", second, err)
	}
	if _, err := s.ChannelDeliveryUpdate(ctx, agent("b"), m.Message.ID, claim.LeaseToken, "ack", "", 0); !errors.Is(err, bus.ErrLeaseTokenMismatch) {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := s.ChannelDeliveryUpdate(ctx, agent("b"), m.Message.ID, second.LeaseToken, "ack", "", 0); err != nil {
			t.Fatal(err)
		}
	}
	inbox, err = s.ChannelInbox(ctx, agent("b"), 10)
	if err != nil || len(inbox) != 0 {
		t.Fatalf("after ack: %v %v", inbox, err)
	}
	m = postChannel(t, s, ctx, agent("a"), c, "revoke-message")
	claim, err = s.ChannelClaim(ctx, agent("b"), m.Message.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelMembershipChange(ctx, agent("a"), c.ID, "b", "remove", "remove", c.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelDeliveryUpdate(ctx, agent("b"), m.Message.ID, claim.LeaseToken, "ack", "", 0); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("revoked lease: %v", err)
	}
}

func TestManagedObserverInboxDoesNotConsume(t *testing.T) {
	s, ctx := channelFixture(t)
	h := bus.ConversationPrincipal{Actor: "human:owner", Run: "gateway", Gateway: true, Admin: true}
	if err := s.RegisterHuman(ctx, h, h.Actor); err != nil {
		t.Fatal(err)
	}
	preview, err := s.SupervisionPreflight(ctx, h, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SupervisionChange(ctx, h, preview, h.Actor, "link"); err != nil {
		t.Fatal(err)
	}
	c := createDM(t, s, ctx, "a", "b", "dm")
	m := postChannel(t, s, ctx, agent("b"), c, "message")
	inbox, err := s.ChannelInbox(ctx, h, 10)
	if err != nil || len(inbox) != 0 {
		t.Fatalf("observer inbox: %v %v", inbox, err)
	}
	if _, err := s.ChannelClaim(ctx, h, m.Message.ID, time.Minute); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("observer claim: %v", err)
	}
	if _, err := s.ChannelClaim(ctx, agent("a"), m.Message.ID, time.Minute); err != nil {
		t.Fatal(err)
	}
	var foreignKeys int
	if err := s.db.QueryRow(`PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("foreign_keys %d: %v", foreignKeys, err)
	}
}
