package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

func TestProtocolWriterKeepsConcurrentFramesIntact(t *testing.T) {
	const frameCount = 200

	var output bytes.Buffer
	writer := newProtocolWriter(&output)
	var group sync.WaitGroup
	for index := 0; index < frameCount; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			if err := writer.encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "notifications/claude/channel",
				"params":  map[string]string{"message_id": fmt.Sprintf("message-%03d", index)},
			}); err != nil {
				t.Errorf("encode frame %d: %v", index, err)
			}
		}(index)
	}
	group.Wait()

	decoder := json.NewDecoder(&output)
	seen := make(map[string]bool, frameCount)
	for decoder.More() {
		var frame struct {
			Params struct {
				MessageID string `json:"message_id"`
			} `json:"params"`
		}
		if err := decoder.Decode(&frame); err != nil {
			t.Fatalf("decode JSON-RPC frame: %v", err)
		}
		if frame.Params.MessageID == "" {
			t.Fatalf("frame has no message ID: %+v", frame)
		}
		seen[frame.Params.MessageID] = true
	}
	if len(seen) != frameCount {
		t.Fatalf("decoded %d unique frames, want %d", len(seen), frameCount)
	}
}
