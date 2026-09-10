package mcp

import (
	"encoding/json"
	"io"
	"sync"
)

// protocolWriter keeps JSON-RPC frames intact when asynchronous server
// notifications and request responses share the MCP stdio transport.
type protocolWriter struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

func newProtocolWriter(output io.Writer) *protocolWriter {
	return &protocolWriter{encoder: json.NewEncoder(output)}
}

func (w *protocolWriter) encode(value interface{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.encoder.Encode(value)
}
