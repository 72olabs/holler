package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/mcp"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestMCPQuestionClaimAckRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	alice, err := mcp.New(db, mcp.Config{
		Actor: "alice", RunID: "alice-run-1", Role: "implementer", Peer: "bob",
		ProjectID: "experiment", ChannelID: "direct",
	})
	if err != nil {
		t.Fatalf("new alice server: %v", err)
	}
	responses := exchange(t, alice,
		request(1, "initialize", map[string]interface{}{"protocolVersion": "2024-11-05"}),
		request(2, "tools/list", map[string]interface{}{}),
		toolCall(3, "bus_send", map[string]interface{}{
			"to": "bob", "body": "Which retry policy applies?", "idempotency_key": "question-1",
		}),
		toolCall(4, "bus_send", map[string]interface{}{
			"to": "bob", "body": "Which retry policy applies?", "idempotency_key": "question-1",
		}),
		toolCallWithMeta(5, "bus_status", map[string]interface{}{}),
	)
	if got := nestedString(t, responses[0], "result", "serverInfo", "name"); got != "holler" {
		t.Fatalf("server name = %q", got)
	}
	tools := nestedSlice(t, responses[1], "result", "tools")
	if len(tools) != 18 {
		t.Fatalf("tool count = %d, want 18", len(tools))
	}
	adoptTool := tools[10].(map[string]interface{})
	annotations := adoptTool["annotations"].(map[string]interface{})
	if adoptTool["name"] != "holler_adopt" || annotations["destructiveHint"] != true || annotations["idempotentHint"] != true {
		t.Fatalf("adoption tool contract = %+v", adoptTool)
	}
	aliasSetTool := tools[13].(map[string]interface{})
	aliasAnnotations := aliasSetTool["annotations"].(map[string]interface{})
	if aliasSetTool["name"] != "holler_alias_set" || aliasAnnotations["destructiveHint"] != true || aliasAnnotations["idempotentHint"] != true {
		t.Fatalf("alias set tool contract = %+v", aliasSetTool)
	}
	firstID := nestedString(t, responses[2], "result", "structuredContent", "message_id")
	secondID := nestedString(t, responses[3], "result", "structuredContent", "message_id")
	if firstID == "" || firstID != secondID {
		t.Fatalf("idempotent send ids = %q, %q", firstID, secondID)
	}

	bob, err := mcp.New(db, mcp.Config{
		Actor: "bob", RunID: "bob-run-1", Role: "owner", Peer: "alice",
		ProjectID: "experiment", ChannelID: "direct",
	})
	if err != nil {
		t.Fatalf("new bob server: %v", err)
	}
	responses = exchange(t, bob,
		toolCall(1, "bus_check_inbox", map[string]interface{}{"_meta": map[string]interface{}{"progressToken": 1}}),
		toolCall(2, "bus_inbox", map[string]interface{}{}),
	)
	metadata := nestedSlice(t, responses[0], "result", "structuredContent", "messages")
	if len(metadata) != 1 {
		t.Fatalf("inbox metadata = %+v", metadata)
	}
	claimed := nestedSlice(t, responses[1], "result", "structuredContent", "messages")
	if len(claimed) != 1 {
		t.Fatalf("claimed messages = %+v", claimed)
	}
	message := claimed[0].(map[string]interface{})
	if message["body"] != "Which retry policy applies?" || message["from"] != "alice" {
		t.Fatalf("claimed message = %+v", message)
	}
	leaseToken := message["lease_token"].(string)
	responses = exchange(t, bob,
		toolCall(3, "bus_ack", map[string]interface{}{"message_id": firstID, "lease_token": leaseToken}),
		toolCall(4, "bus_status", map[string]interface{}{}),
	)
	if got := nestedNumber(t, responses[1], "result", "structuredContent", "unread"); got != 0 {
		t.Fatalf("unread after ack = %v", got)
	}
}

func TestCapabilityBridgeSurvivesDaemonUpgradeWithoutReplacingMCPServer(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(directory, "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	socketDirectory, err := os.MkdirTemp("/tmp", "holler-cap-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
	socket := filepath.Join(socketDirectory, "holler.sock")

	cancelOld, oldDone := serveCapabilityDaemon(t, socket, db)
	client, err := api.Dial(ctx, socket, api.Identity{
		Actor: "operator", RunID: "operator-run", Client: "mcp-upgrade-test",
		ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := mcp.New(client, mcp.Config{Actor: "operator", RunID: "operator-run", ProjectID: "coupon"})
	if err != nil {
		t.Fatal(err)
	}
	callMCP := persistentMCP(t, server)

	response := callMCP(toolCall(1, "holler_capabilities", map[string]interface{}{}))
	if capabilityNamed(t, response, "future.echo") {
		t.Fatal("old daemon unexpectedly advertised future.echo")
	}

	cancelOld()
	if err := <-oldDone; err != nil {
		t.Fatalf("stop old daemon: %v", err)
	}
	cancelNew, newDone := serveCapabilityDaemon(t, socket, db, api.WithCapability(bus.CapabilityDescriptor{
		Name: "future.echo", Mode: bus.CapabilityRead, Since: "future",
		Description: "Echo a value to prove daemon-owned capability evolution.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}`),
	}, func(_ context.Context, _ api.Store, _ api.Identity, raw json.RawMessage) (interface{}, error) {
		var args struct {
			Value string `json:"value"`
		}
		if err := json.Unmarshal(raw, &args); err != nil {
			return nil, err
		}
		return map[string]string{"echo": args.Value, "daemon": "upgraded"}, nil
	}))
	defer func() {
		cancelNew()
		if err := <-newDone; err != nil {
			t.Errorf("stop new daemon: %v", err)
		}
	}()

	// The MCP Server and API Client above are deliberately unchanged. The
	// client's ordinary reconnect path reaches the replacement daemon.
	responses := []map[string]interface{}{
		callMCP(toolCall(2, "holler_capabilities", map[string]interface{}{})),
		callMCP(toolCall(3, "holler_read", map[string]interface{}{
			"capability": "future.echo", "arguments": map[string]interface{}{"value": "still here"},
		})),
		callMCP(toolCall(4, "holler_write", map[string]interface{}{
			"capability": "future.echo", "arguments": map[string]interface{}{"value": "wrong lane"},
		})),
		callMCP(toolCall(5, "holler_read", map[string]interface{}{
			"capability": "alias.set", "arguments": map[string]interface{}{
				"alias": "reviewer", "actor": "operator", "idempotency_key": "wrong-lane",
			},
		})),
		callMCP(toolCall(6, "holler_write", map[string]interface{}{
			"capability": "alias.set", "arguments": map[string]interface{}{
				"alias": "reviewer", "actor": "operator", "idempotency_key": "bridge-alias-set",
			},
		})),
		callMCP(toolCall(7, "holler_read", map[string]interface{}{
			"capability": "alias.resolve", "arguments": map[string]interface{}{"alias": "reviewer"},
		})),
	}
	if !capabilityNamed(t, responses[0], "future.echo") {
		t.Fatal("unchanged MCP server did not discover upgraded daemon capability")
	}
	if got := nestedString(t, responses[1], "result", "structuredContent", "echo"); got != "still here" {
		t.Fatalf("future.echo result = %q", got)
	}
	for index, label := range []string{"read capability through write bridge", "write capability through read bridge"} {
		if responses[index+2]["error"] == nil {
			t.Fatalf("%s was accepted: %+v", label, responses[index+2])
		}
	}
	if got := nestedString(t, responses[5], "result", "structuredContent", "actor"); got != "operator" {
		t.Fatalf("alias.resolve through bridge actor = %q", got)
	}
	if got := nestedString(t, responses[5], "result", "structuredContent", "project_id"); got != "coupon" {
		t.Fatalf("alias.resolve through bridge project = %q", got)
	}
}

func persistentMCP(t *testing.T, server *mcp.Server) func(map[string]interface{}) map[string]interface{} {
	t.Helper()
	inputReader, inputWriter := io.Pipe()
	outputReader, outputWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		err := server.Run(ctx, inputReader, outputWriter)
		_ = outputWriter.Close()
		done <- err
	}()
	encoder := json.NewEncoder(inputWriter)
	decoder := json.NewDecoder(outputReader)
	t.Cleanup(func() {
		cancel()
		_ = inputWriter.Close()
		_ = outputReader.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
				t.Errorf("persistent MCP server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("persistent MCP server did not stop")
		}
	})
	return func(request map[string]interface{}) map[string]interface{} {
		t.Helper()
		if err := encoder.Encode(request); err != nil {
			t.Fatalf("encode persistent MCP request: %v", err)
		}
		var response map[string]interface{}
		if err := decoder.Decode(&response); err != nil {
			t.Fatalf("decode persistent MCP response: %v", err)
		}
		return response
	}
}

func serveCapabilityDaemon(t *testing.T, socket string, db *store.Store, options ...api.ServerOption) (context.CancelFunc, <-chan error) {
	t.Helper()
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- api.NewServer(db, options...).Serve(ctx, listener) }()
	return cancel, done
}

func capabilityNamed(t *testing.T, response map[string]interface{}, name string) bool {
	t.Helper()
	capabilities := nestedSlice(t, response, "result", "structuredContent", "capabilities")
	for _, value := range capabilities {
		capability, ok := value.(map[string]interface{})
		if ok && capability["name"] == name {
			return true
		}
	}
	return false
}

func TestMCPRejectsModelSuppliedSenderIdentity(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	server, err := mcp.New(db, mcp.Config{Actor: "alice", RunID: "run-1"})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	responses := exchange(t, server, toolCall(1, "bus_send", map[string]interface{}{
		"from": "mallory", "to": "bob", "body": "spoofed",
	}))
	errorValue := responses[0]["error"].(map[string]interface{})
	if !strings.Contains(errorValue["message"].(string), "unknown field") {
		t.Fatalf("spoof error = %+v", errorValue)
	}
}

func TestMCPSendNotifiesAfterCommitButNotOnIdempotentReplay(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	server, err := mcp.New(db, mcp.Config{
		Actor: "alice", RunID: "run-1", ProjectID: "experiment",
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	responses := exchange(t, server,
		toolCall(1, "bus_send", map[string]interface{}{
			"to": "bob", "body": "hello", "idempotency_key": "stable-send",
		}),
		toolCall(2, "bus_send", map[string]interface{}{
			"to": "bob", "body": "hello", "idempotency_key": "stable-send",
		}),
	)
	job, err := db.ClaimNotification(ctx)
	if err != nil || job.RecipientActor != "bob" {
		t.Fatalf("notification job = %+v, err = %v", job, err)
	}
	if duplicate := nestedValue(t, responses[0], "result", "structuredContent", "duplicate"); duplicate != false {
		t.Fatalf("first duplicate = %v", duplicate)
	}
	if duplicate := nestedValue(t, responses[1], "result", "structuredContent", "duplicate"); duplicate != true {
		t.Fatalf("replay duplicate = %v", duplicate)
	}
}

func TestMCPProfileAndDiscoveryRemainAdvisory(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	server, err := mcp.New(db, mcp.Config{
		Actor: "coupon-reviewer", RunID: "review-run-1", ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	responses := exchange(t, server,
		toolCall(1, "holler_profile", map[string]interface{}{
			"role_text": "Reviews coupon correctness", "accepts": []string{"REVIEW_REQUEST"},
		}),
		toolCall(2, "holler_profile", map[string]interface{}{
			"role_text": "Reviews coupon correctness", "accepts": []string{"REVIEW_REQUEST"},
		}),
		toolCall(3, "holler_who", map[string]interface{}{}),
		toolCall(4, "holler_profile", map[string]interface{}{
			"role_text": "Reviews coupon correctness", "direction": "both",
		}),
	)
	if got := nestedValue(t, responses[0], "result", "structuredContent", "updated"); got != true {
		t.Fatalf("initial profile updated = %v", got)
	}
	if got := nestedValue(t, responses[1], "result", "structuredContent", "updated"); got != false {
		t.Fatalf("identical profile updated = %v", got)
	}
	actors := nestedSlice(t, responses[2], "result", "structuredContent", "actors")
	if len(actors) != 1 {
		t.Fatalf("directory actors = %+v", actors)
	}
	actor := actors[0].(map[string]interface{})
	profile := actor["profile"].(map[string]interface{})
	if actor["actor"] != "coupon-reviewer" || profile["role_text"] != "Reviews coupon correctness" {
		t.Fatalf("directory actor = %+v", actor)
	}
	if got := nestedString(t, responses[2], "result", "structuredContent", "metadata_trust"); got != "untrusted" {
		t.Fatalf("metadata_trust = %q", got)
	}
	errorValue := responses[3]["error"].(map[string]interface{})
	if !strings.Contains(errorValue["message"].(string), `unknown field "direction"`) {
		t.Fatalf("direction error = %+v", errorValue)
	}
}

func TestMCPAdoptRequiresLiveBoundActorAndPreservesProvenance(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "replacement", RunID: "replacement-run", Harness: "test", SessionID: "replacement-session",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	sent, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "mcp-orphan", ProjectID: "coupon", ChannelID: "direct",
		FromActor: "sender", FromRun: "sender-run", ToActors: []string{"reviewer-old"}, Type: "REVIEW_REQUEST",
		Body: json.RawMessage(`{"text":"review"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := mcp.New(db, mcp.Config{Actor: "replacement", RunID: "replacement-run", ProjectID: "coupon"})
	if err != nil {
		t.Fatal(err)
	}
	responses := exchange(t, server,
		toolCall(1, "holler_adopt", map[string]interface{}{
			"source_actor": "reviewer-old", "idempotency_key": "mcp-adopt-once",
		}),
		toolCall(2, "bus_inbox", map[string]interface{}{}),
	)
	if got := nestedString(t, responses[0], "result", "structuredContent", "adopting_actor"); got != "replacement" {
		t.Fatalf("adopting actor = %q", got)
	}
	messages := nestedSlice(t, responses[1], "result", "structuredContent", "messages")
	if len(messages) != 1 {
		t.Fatalf("messages = %+v", messages)
	}
	message := messages[0].(map[string]interface{})
	if message["message_id"] != sent.Message.ID || message["recipient_actor"] != "replacement" || message["original_recipient_actor"] != "reviewer-old" {
		t.Fatalf("adopted message = %+v", message)
	}
}

func TestMCPAliasToolsRouteToCanonicalActor(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "test", SessionID: "claude-session",
		ProjectID: "default", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	operator, err := mcp.New(db, mcp.Config{Actor: "operator", RunID: "operator-run", ProjectID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	responses := exchange(t, operator,
		toolCall(1, "holler_aliases", map[string]interface{}{}),
		toolCall(2, "holler_alias_set", map[string]interface{}{
			"alias": "skillbank", "actor": "claude", "idempotency_key": "mcp-alias-set",
		}),
		toolCall(3, "holler_alias_resolve", map[string]interface{}{"alias": "skillbank"}),
		toolCall(4, "holler_aliases", map[string]interface{}{}),
	)
	emptyAliases := nestedSlice(t, responses[0], "result", "structuredContent", "aliases")
	if len(emptyAliases) != 0 {
		t.Fatalf("empty aliases = %+v", emptyAliases)
	}
	if got := nestedString(t, responses[1], "result", "structuredContent", "alias", "actor"); got != "claude" {
		t.Fatalf("set alias actor = %q", got)
	}
	if got := nestedString(t, responses[2], "result", "structuredContent", "actor"); got != "claude" {
		t.Fatalf("resolved actor = %q", got)
	}
	aliases := nestedSlice(t, responses[3], "result", "structuredContent", "aliases")
	if len(aliases) != 1 {
		t.Fatalf("aliases = %+v", aliases)
	}
	sender, err := mcp.New(db, mcp.Config{Actor: "codex", RunID: "codex-run", ProjectID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	responses = exchange(t, sender, toolCall(1, "bus_send", map[string]interface{}{
		"to": "skillbank", "body": "review", "idempotency_key": "mcp-alias-send",
	}))
	recipients := nestedSlice(t, responses[0], "result", "structuredContent", "to")
	if len(recipients) != 1 || recipients[0] != "claude" {
		t.Fatalf("canonical recipients = %+v", recipients)
	}
}

func TestToolSurfaceIdentityIsStable(t *testing.T) {
	wantNames := []string{
		"bus_send", "bus_check_inbox", "bus_claim", "bus_inbox", "bus_ack", "bus_extend", "bus_nack", "bus_status",
		"holler_profile", "holler_who", "holler_adopt", "holler_aliases", "holler_alias_resolve",
		"holler_alias_set", "holler_alias_remove", "holler_capabilities", "holler_read", "holler_write",
	}
	if got := strings.Join(mcp.ToolNames(), ","); got != strings.Join(wantNames, ",") {
		t.Fatalf("tool names = %q", got)
	}
	const wantHash = "sha256:e1eae352b2d6000bd8ad564424b3d3b7d25ebf4c24eb5f88add5e380d6973b23"
	if got := mcp.ToolSurfaceHash(); got != wantHash {
		t.Fatalf("tool surface hash = %q, want %q; connector reauthorization is required for an intentional schema change", got, wantHash)
	}
}

func exchange(t *testing.T, server *mcp.Server, requests ...map[string]interface{}) []map[string]interface{} {
	t.Helper()
	var input, output bytes.Buffer
	encoder := json.NewEncoder(&input)
	for _, req := range requests {
		if err := encoder.Encode(req); err != nil {
			t.Fatalf("encode request: %v", err)
		}
	}
	if err := server.Run(context.Background(), &input, &output); err != nil {
		t.Fatalf("run server: %v", err)
	}
	var responses []map[string]interface{}
	decoder := json.NewDecoder(&output)
	for decoder.More() {
		var response map[string]interface{}
		if err := decoder.Decode(&response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		responses = append(responses, response)
	}
	if len(responses) != len(requests) {
		t.Fatalf("responses=%d requests=%d output=%s", len(responses), len(requests), output.String())
	}
	return responses
}

func request(id int, method string, params interface{}) map[string]interface{} {
	return map[string]interface{}{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
}

func toolCall(id int, name string, arguments interface{}) map[string]interface{} {
	return request(id, "tools/call", map[string]interface{}{"name": name, "arguments": arguments})
}

func toolCallWithMeta(id int, name string, arguments interface{}) map[string]interface{} {
	return request(id, "tools/call", map[string]interface{}{
		"name": name, "arguments": arguments, "_meta": map[string]interface{}{"progressToken": id},
	})
}

func nestedValue(t *testing.T, root interface{}, path ...string) interface{} {
	t.Helper()
	current := root
	for _, name := range path {
		object, ok := current.(map[string]interface{})
		if !ok {
			t.Fatalf("%v is not an object while reading %v", current, path)
		}
		current, ok = object[name]
		if !ok {
			t.Fatalf("missing %q while reading %v in %+v", name, path, root)
		}
	}
	return current
}

func nestedString(t *testing.T, root interface{}, path ...string) string {
	t.Helper()
	value, ok := nestedValue(t, root, path...).(string)
	if !ok {
		t.Fatalf("value at %v is not string", path)
	}
	return value
}

func nestedSlice(t *testing.T, root interface{}, path ...string) []interface{} {
	t.Helper()
	value, ok := nestedValue(t, root, path...).([]interface{})
	if !ok {
		t.Fatalf("value at %v is not slice", path)
	}
	return value
}

func nestedNumber(t *testing.T, root interface{}, path ...string) float64 {
	t.Helper()
	value, ok := nestedValue(t, root, path...).(float64)
	if !ok {
		t.Fatalf("value at %v is not number", path)
	}
	return value
}
