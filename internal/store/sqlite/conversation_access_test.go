package sqlite

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

func TestSupervisionEnrollmentJoinForwardAndRevocation(t *testing.T) {
	s, ctx := channelFixture(t)
	admin := bus.ConversationPrincipal{Actor: "human:owner", Run: "gateway", Gateway: true, Admin: true}
	if err := s.RegisterHuman(ctx, admin, admin.Actor); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterHuman(ctx, agent("a"), "human:forged"); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("agent enrolled human: %v", err)
	}
	c := createDM(t, s, ctx, "a", "b", "dm")
	old := postChannel(t, s, ctx, agent("a"), c, "old")
	preview, err := s.SupervisionPreflight(ctx, admin, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SupervisionChange(ctx, agent("a"), preview, admin.Actor, "forge"); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("agent supervised itself: %v", err)
	}
	current, err := s.SupervisionChange(ctx, admin, preview, admin.Actor, "link")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelMessageGet(ctx, admin, old.Message.ID); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("retroactive history: %v", err)
	}
	c, err = s.ChannelGet(ctx, agent("a"), c.ID)
	if err != nil {
		t.Fatal(err)
	}
	fresh := postChannel(t, s, ctx, agent("a"), c, "fresh")
	if _, err := s.ChannelMessageGet(ctx, admin, fresh.Message.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SupervisionChange(ctx, admin, preview, admin.Actor, "stale"); !errors.Is(err, bus.ErrAudienceChanged) {
		t.Fatalf("stale policy accepted: %v", err)
	}
	if _, err := s.SupervisionChange(ctx, admin, current, "", "unlink"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelMessageGet(ctx, admin, fresh.Message.ID); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("observer retained rights: %v", err)
	}
}

func TestNamedMembershipAndPersonalState(t *testing.T) {
	s, ctx := channelFixture(t)
	c, err := s.ChannelCreate(ctx, agent("a"), bus.ChannelCreate{ProjectID: "test", Kind: "named", Title: "Decision", Participants: []string{"a", "b"}, IdempotencyKey: "group"})
	if err != nil {
		t.Fatal(err)
	}
	old := postChannel(t, s, ctx, agent("a"), c, "old")
	c, err = s.ChannelMembershipChange(ctx, agent("a"), c.ID, "c", "add", "admit", c.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelMessageGet(ctx, agent("c"), old.Message.ID); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("new member read old history: %v", err)
	}
	m := postChannel(t, s, ctx, agent("a"), c, "new")
	v, err := s.ChannelViewGet(ctx, agent("c"), c.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	v.ReadThrough = m.Message.Seq
	snooze := time.Now().Add(time.Hour)
	v.SnoozedUntil = &snooze
	v.Archived = true
	v.UnreadFrom = &m.Message.Seq
	v, err = s.ChannelViewUpdate(ctx, agent("c"), v)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := s.ChannelViewGet(ctx, agent("c"), c.ID, "")
	if err != nil || reloaded.Revision != 1 || reloaded.ReadThrough != m.Message.Seq || !reloaded.Archived || reloaded.UnreadFrom == nil {
		t.Fatalf("view roundtrip: %+v %v", reloaded, err)
	}
	other, err := s.ChannelViewGet(ctx, agent("b"), c.ID, "")
	if err != nil || other.Revision != 0 || other.ReadThrough != 0 {
		t.Fatalf("shared personal state: %+v %v", other, err)
	}
	v.Revision = 0
	if _, err := s.ChannelViewUpdate(ctx, agent("c"), v); !errors.Is(err, bus.ErrAudienceChanged) {
		t.Fatalf("stale preference accepted: %v", err)
	}
	before := c.Revision
	c, err = s.ChannelMembershipChange(ctx, agent("a"), c.ID, "c", "remove", "remove", c.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelMessageGet(ctx, agent("c"), m.Message.ID); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("removed member reads: %v", err)
	}
	if _, err := s.ChannelPost(ctx, agent("a"), bus.ChannelPost{ChannelID: c.ID, ExpectedRevision: before, IdempotencyKey: "stale-post", Body: json.RawMessage(`{}`)}); !errors.Is(err, bus.ErrAudienceChanged) {
		t.Fatalf("stale audience post: %v", err)
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM managed_deliveries WHERE message_id=? AND recipient_actor='c'`, m.Message.ID).Scan(&state); err != nil || state != "revoked" {
		t.Fatalf("pending delivery not revoked: %s %v", state, err)
	}
}

func TestReservedHumanNamespaceAndLegacySend(t *testing.T) {
	s, ctx := channelFixture(t)
	if err := s.ReserveActorName(ctx, "human:h"); !errors.Is(err, bus.ErrChannelCapability) {
		t.Fatal(err)
	}
	for _, mode := range []bus.NameMode{bus.NameModeExact, bus.NameModeAllocate} {
		var handles []string
		if mode == bus.NameModeAllocate {
			handles = []string{"session:human-spoof"}
		}
		if _, err := s.BindActor(ctx, bus.ActorBindRequest{RequestedActor: "human:h", RunID: "r", NameMode: mode, ContinuityHandles: handles}); !errors.Is(err, bus.ErrChannelCapability) {
			t.Fatalf("human legacy binding: %v", err)
		}
	}
	for _, target := range []string{"human:h", "human:owner"} {
		_, err := s.Send(ctx, bus.SendRequest{FromActor: "a", FromRun: "r", ToActors: []string{target}, ProjectID: "test", ChannelID: "direct", IdempotencyKey: target, Type: "MESSAGE", Body: json.RawMessage(`{}`)})
		if !errors.Is(err, bus.ErrChannelCapability) {
			t.Fatalf("human legacy send: %v", err)
		}
	}
	for _, pair := range [][2]string{{"human:alias", "a"}, {"helper", "human:h"}} {
		_, err := s.SetAlias(ctx, bus.AliasSetRequest{Alias: pair[0], Actor: pair[1], UpdatedByActor: "operator", UpdatedByRun: "r", ProjectID: "test", IdempotencyKey: pair[0]})
		if !errors.Is(err, bus.ErrChannelCapability) {
			t.Fatalf("human alias: %v", err)
		}
	}
	// A pre-existing alias from an older database cannot bypass send-time checks.
	if _, err := s.db.Exec(`INSERT INTO actor_aliases VALUES('old-human-route','human:h',1,'operator','r','test','old',1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Send(ctx, bus.SendRequest{FromActor: "a", FromRun: "r", Destinations: []bus.Route{{Kind: bus.RouteAlias, Value: "old-human-route"}}, ProjectID: "test", ChannelID: "direct", IdempotencyKey: "alias-send", Type: "MESSAGE", Body: json.RawMessage(`{}`)}); !errors.Is(err, bus.ErrChannelCapability) {
		t.Fatalf("legacy alias bypass: %v", err)
	}
}
