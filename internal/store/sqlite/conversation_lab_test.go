package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/72olabs/holler/internal/bus"
	"path/filepath"
	"testing"
	"time"
)

// Deterministic local Studio lab: this simulates H, not a live human canary.
func TestConversationStudioLabRestartAndReturn(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "studio.sqlite")
	now := time.Date(2026, 9, 14, 8, 0, 0, 0, time.UTC)
	s, err := Open(ctx, path, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	for _, actor := range []string{"a", "b", "stranger"} {
		if err := s.ReserveActorName(ctx, actor); err != nil {
			t.Fatal(err)
		}
	}
	h := bus.ConversationPrincipal{Actor: "human:lab", Run: "lab-human-session", Gateway: true, Admin: true}
	if err := s.RegisterHuman(ctx, h, h.Actor); err != nil {
		t.Fatal(err)
	}
	enroll, err := s.SupervisionPreflight(ctx, h, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SupervisionChange(ctx, h, enroll, h.Actor, "enroll"); err != nil {
		t.Fatal(err)
	}
	dm := createDM(t, s, ctx, "a", "b", "agents")
	question, err := s.ChannelPost(ctx, agent("a"), bus.ChannelPost{ChannelID: dm.ID, ExpectedRevision: dm.Revision, IdempotencyKey: "design", Body: json.RawMessage(`{"question":"Choose the retry policy","context":"Two agents need a decision","risks":"Duplicate work","tradeoffs":"Latency versus retries"}`), Attention: []string{"b"}, Respondent: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelHistoryPage(ctx, h, dm.ID, "", "", 50); err != nil {
		t.Fatal(err)
	}
	inbox, err := s.ChannelInbox(ctx, agent("b"), 10)
	if err != nil || len(inbox) != 1 || inbox[0].Attempt != 0 {
		t.Fatal("observation consumed work")
	}
	privatePreview, err := s.ChannelContinuationPreflight(ctx, h, ContinuationIntent{SourceMessageID: question.Message.ID, Create: &bus.ChannelCreate{ProjectID: "test", Kind: "dm", Participants: []string{h.Actor, "a"}}, Body: json.RawMessage(`"What is the failure risk?"`)})
	if err != nil {
		t.Fatal(err)
	}
	private, err := s.ChannelContinuationCommit(ctx, h, privatePreview.Token, "private")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelMessageGet(ctx, agent("b"), private.Message.ID); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatal("private question leaked")
	}
	groupPreview, err := s.ChannelContinuationPreflight(ctx, h, ContinuationIntent{SourceMessageID: question.Message.ID, Create: &bus.ChannelCreate{ProjectID: "test", Kind: "named", Title: "Decision", Participants: []string{h.Actor, "a", "b"}}, Body: json.RawMessage(`"B, what do you recommend?"`), Attention: []string{"b"}, Respondent: "b"})
	if err != nil {
		t.Fatal(err)
	}
	group, err := s.ChannelContinuationCommit(ctx, h, groupPreview.Token, "group")
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.ChannelGet(ctx, h, group.Message.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := s.ChannelClaim(ctx, agent("b"), group.Message.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	answer, err := s.ChannelPost(ctx, agent("b"), bus.ChannelPost{ChannelID: c.ID, ExpectedRevision: c.Revision, IdempotencyKey: "recommend", Body: json.RawMessage(`"Use bounded retries"`), ResponseTo: group.Message.ResponseID, ExpectedResponseRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	if answer.Message.ThreadID != group.Message.ThreadID {
		t.Fatal("answer lost question thread")
	}
	if _, err := s.ChannelDeliveryUpdate(ctx, agent("b"), group.Message.ID, claim.LeaseToken, "ack", "", 0); err != nil {
		t.Fatal(err)
	}
	decision, err := s.ChannelPost(ctx, h, bus.ChannelPost{ChannelID: c.ID, InReplyTo: answer.Message.ID, ExpectedRevision: c.Revision, IdempotencyKey: "decision", Body: json.RawMessage(`{"text":"Proceed with bounded retries","kind":"studio.decision"}`)})
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.ChannelViewGet(ctx, h, c.ID, group.Message.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	until := now.Add(time.Hour)
	view.ReadThrough = decision.Message.Seq
	view.SnoozedUntil = &until
	view.UnreadFrom = &group.Message.Seq
	if _, err := s.ChannelViewUpdate(ctx, h, view); err != nil {
		t.Fatal(err)
	}
	source, err := s.ChannelGet(ctx, agent("a"), dm.ID)
	if err != nil || source.LastSeq != question.Message.Seq {
		t.Fatal("side discussions wrote source")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(ctx, path, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	view, err = s.ChannelViewGet(ctx, h, c.ID, group.Message.ThreadID)
	if err != nil || view.ReadThrough != decision.Message.Seq || view.UnreadFrom == nil || !view.SnoozedUntil.Equal(until) {
		t.Fatalf("return state lost: %+v %v", view, err)
	}
	requests, err := s.ChannelResponses(ctx, h, c.ID)
	if err != nil || len(requests) != 1 || requests[0].State != "answered" || requests[0].AnswerID != answer.Message.ID {
		t.Fatalf("response lost: %+v %v", requests, err)
	}
	if _, err := s.ChannelMessageGet(ctx, agent("stranger"), decision.Message.ID); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatal("stranger read lab")
	}
	revoke, err := s.SupervisionPreflight(ctx, h, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SupervisionChange(ctx, h, revoke, "", "revoke"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelMessageGet(ctx, h, question.Message.ID); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatal("revoked observer kept source access")
	}
	if _, err := s.ChannelMessageGet(ctx, h, decision.Message.ID); err != nil {
		t.Fatal("independent human participation lost")
	}
	// A's pending group delivery survives independently of B's ACK and restart.
	pending, err := s.ChannelInbox(ctx, agent("a"), 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range pending {
		found = found || d.Message.ID == group.Message.ID
	}
	if !found {
		t.Fatal("B's ACK consumed A's work")
	}
}
