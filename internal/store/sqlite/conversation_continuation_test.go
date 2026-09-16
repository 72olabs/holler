package sqlite

import (
	"encoding/json"
	"errors"
	"github.com/72olabs/holler/internal/bus"
	"testing"
	"time"
)

func TestContinuationAtomicPreviewPrivacyAndRetry(t *testing.T) {
	s, ctx := channelFixture(t)
	sourceChannel := createDM(t, s, ctx, "a", "b", "source")
	source := postChannel(t, s, ctx, agent("a"), sourceChannel, "source-msg")
	intent := ContinuationIntent{SourceMessageID: source.Message.ID, Create: &bus.ChannelCreate{ProjectID: "test", Kind: "named", Title: "side discussion", Participants: []string{"a", "c"}}, Body: json.RawMessage(`"independent question"`)}
	preview, err := s.ChannelContinuationPreflight(ctx, agent("a"), intent)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM channels`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("preview wrote: %d %v", count, err)
	}
	if _, err := s.ChannelContinuationCommit(ctx, agent("b"), preview.Token, "wrong-principal"); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatal(err)
	}
	if _, err := s.ChannelContinuationCommit(ctx, agent("a"), preview.Token+"x", "tampered"); !errors.Is(err, bus.ErrPreflightExpired) {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_continuation BEFORE INSERT ON channel_references BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelContinuationCommit(ctx, agent("a"), preview.Token, "continue"); err == nil {
		t.Fatal("injected write succeeded")
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM channels`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("partial transaction: %d %v", count, err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_continuation`); err != nil {
		t.Fatal(err)
	}
	result, err := s.ChannelContinuationCommit(ctx, agent("a"), preview.Token, "continue")
	if err != nil {
		t.Fatal(err)
	}
	projected, err := s.ChannelMessageGet(ctx, agent("c"), result.Message.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected.References) != 1 || projected.References[0].Available || projected.References[0].SourceMessageID != "" {
		t.Fatalf("source leak: %+v", projected)
	}
	old, err := s.ChannelGet(ctx, agent("a"), sourceChannel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if old.LastSeq != source.Message.Seq {
		t.Fatal("private continuation wrote source backlink")
	}
	if _, err := s.ChannelMessageGet(ctx, agent("b"), result.Message.ID); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatal(err)
	}
	// A restarted signing secret invalidates previews but cannot break committed retry.
	s.conversationSecret[0] ^= 1
	retry, err := s.ChannelContinuationCommit(ctx, agent("a"), preview.Token, "continue")
	if err != nil || !retry.Duplicate || retry.Message.ID != result.Message.ID {
		t.Fatalf("retry: %+v %v", retry, err)
	}
}

func TestContinuationStaleAudienceExpiryAndHistoryCursor(t *testing.T) {
	s, ctx := channelFixture(t)
	sourceChannel := createDM(t, s, ctx, "a", "b", "source")
	source := postChannel(t, s, ctx, agent("a"), sourceChannel, "source-msg")
	dest, err := s.ChannelCreate(ctx, agent("a"), bus.ChannelCreate{ProjectID: "test", Kind: "named", Title: "destination", Participants: []string{"a", "c"}, IdempotencyKey: "dest"})
	if err != nil {
		t.Fatal(err)
	}
	intent := ContinuationIntent{SourceMessageID: source.Message.ID, DestinationID: dest.ID, Body: json.RawMessage(`"question"`)}
	preview, err := s.ChannelContinuationPreflight(ctx, agent("a"), intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelMembershipChange(ctx, agent("a"), dest.ID, "d", "add", "add", dest.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelContinuationCommit(ctx, agent("a"), preview.Token, "stale"); !errors.Is(err, bus.ErrAudienceChanged) {
		t.Fatalf("stale: %v", err)
	}
	preview, err = s.ChannelContinuationPreflight(ctx, agent("a"), intent)
	if err != nil {
		t.Fatal(err)
	}
	now := s.now
	s.now = func() time.Time { return preview.ExpiresAt.Add(time.Second) }
	if _, err := s.ChannelContinuationCommit(ctx, agent("a"), preview.Token, "expired"); !errors.Is(err, bus.ErrPreflightExpired) {
		t.Fatalf("expiry: %v", err)
	}
	s.now = now
	postChannel(t, s, ctx, agent("a"), sourceChannel, "second")
	page, err := s.ChannelHistoryPage(ctx, agent("a"), sourceChannel.ID, "", "", 1)
	if err != nil || len(page.Messages) != 1 || page.NextCursor == "" {
		t.Fatalf("page: %+v %v", page, err)
	}
	if _, err := s.ChannelHistoryPage(ctx, agent("b"), sourceChannel.ID, "", page.NextCursor, 1); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("cursor identity: %v", err)
	}
	next, err := s.ChannelHistoryPage(ctx, agent("a"), sourceChannel.ID, "", page.NextCursor, 1)
	if err != nil || len(next.Messages) != 1 || next.Messages[0].ID == source.Message.ID {
		t.Fatalf("next: %+v %v", next, err)
	}
}
