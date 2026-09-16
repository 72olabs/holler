package sqlite

import (
	"encoding/json"
	"errors"
	"github.com/72olabs/holler/internal/bus"
	"testing"
)

func TestConversationReviewNamespaceAndMissingPrincipal(t *testing.T) {
	s, ctx := channelFixture(t)
	for _, name := range []string{"human:h", "Human:h", "HUMAN:h", " human:h "} {
		for _, mode := range []bus.NameMode{bus.NameModeExact, bus.NameModeAllocate} {
			for _, handles := range [][]string{nil, {"session:spoof"}} {
				if _, err := s.BindActor(ctx, bus.ActorBindRequest{RequestedActor: name, RunID: "r", NameMode: mode, ContinuityHandles: handles}); !errors.Is(err, bus.ErrChannelCapability) {
					t.Fatalf("%q %s %v: %v", name, mode, handles, err)
				}
			}
		}
		if err := s.ReserveActorName(ctx, name); !errors.Is(err, bus.ErrChannelCapability) {
			t.Fatal(err)
		}
	}
	if _, err := s.ListEvents(ctx, "test", "operational", 0, 10); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("missing principal: %v", err)
	}
}

func TestConversationRegrantPreservesAuditNotOldRights(t *testing.T) {
	s, ctx := channelFixture(t)
	c, err := s.ChannelCreate(ctx, agent("a"), bus.ChannelCreate{ProjectID: "test", Kind: "named", Title: "review", Participants: []string{"a", "b"}, IdempotencyKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	old := postChannel(t, s, ctx, agent("a"), c, "old")
	c, err = s.ChannelMembershipChange(ctx, agent("a"), c.ID, "b", "remove", "remove", c.Revision)
	if err != nil {
		t.Fatal(err)
	}
	gap := postChannel(t, s, ctx, agent("a"), c, "gap")
	c, err = s.ChannelMembershipChange(ctx, agent("a"), c.ID, "b", "add", "rejoin", c.Revision)
	if err != nil {
		t.Fatal(err)
	}
	fresh := postChannel(t, s, ctx, agent("a"), c, "fresh")
	for _, id := range []string{old.Message.ID, gap.Message.ID} {
		if _, err := s.ChannelMessageGet(ctx, agent("b"), id); !errors.Is(err, bus.ErrChannelDenied) {
			t.Fatalf("restored %s: %v", id, err)
		}
	}
	if _, err := s.ChannelMessageGet(ctx, agent("b"), fresh.Message.ID); err != nil {
		t.Fatal(err)
	}
	var total, revoked int
	if err := s.db.QueryRow(`SELECT COUNT(*),SUM(revoked_seq IS NOT NULL) FROM channel_grants WHERE channel_id=? AND actor='b' AND source='participant'`, c.ID).Scan(&total, &revoked); err != nil || total != 2 || revoked != 1 {
		t.Fatalf("audit %d/%d: %v", total, revoked, err)
	}
}

func TestConversationResponseCannotCrossThread(t *testing.T) {
	s, ctx := channelFixture(t)
	c := createDM(t, s, ctx, "a", "b", "dm")
	q, err := s.ChannelPost(ctx, agent("a"), bus.ChannelPost{ChannelID: c.ID, ExpectedRevision: c.Revision, IdempotencyKey: "q", Body: json.RawMessage(`"question"`), Respondent: "b"})
	if err != nil {
		t.Fatal(err)
	}
	other := postChannel(t, s, ctx, agent("a"), c, "other")
	answer := bus.ChannelPost{ChannelID: c.ID, ThreadID: other.Message.ThreadID, ExpectedRevision: c.Revision, IdempotencyKey: "answer", Body: json.RawMessage(`"answer"`), ResponseTo: q.Message.ResponseID, ExpectedResponseRevision: 1}
	if _, err := s.ChannelPost(ctx, agent("b"), answer); !errors.Is(err, bus.ErrResponseConflict) {
		t.Fatalf("cross-thread answer: %v", err)
	}
	answer.ThreadID = ""
	got, err := s.ChannelPost(ctx, agent("b"), answer)
	if err != nil {
		t.Fatal(err)
	}
	if got.Message.ThreadID != q.Message.ThreadID || got.Message.InReplyTo != q.Message.ID {
		t.Fatalf("lost request linkage: %+v", got)
	}
}
