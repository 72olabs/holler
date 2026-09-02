package bus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// CapabilityMode separates daemon capabilities by the authority their MCP
// bridge is allowed to exercise. Callers never choose or override this value.
type CapabilityMode string

const (
	CapabilityRead           CapabilityMode = "read"
	CapabilityWrite          CapabilityMode = "write"
	maxCapabilitySchemaBytes                = 64 * 1024
)

// CapabilityDescriptor is the daemon-owned, typed catalog entry exposed to
// long-lived clients. InputSchema is a JSON Schema object for Arguments.
type CapabilityDescriptor struct {
	Name        string          `json:"name"`
	Mode        CapabilityMode  `json:"mode"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
	Since       string          `json:"since,omitempty"`
}

// CapabilityInvocation requests one catalog operation through the bridge.
// The server looks up the operation's mode; the caller cannot supply it.
type CapabilityInvocation struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func ValidateCapabilityDescriptor(descriptor CapabilityDescriptor) error {
	descriptor.Name = strings.TrimSpace(descriptor.Name)
	descriptor.Description = strings.TrimSpace(descriptor.Description)
	if err := validateCapabilityName(descriptor.Name); err != nil {
		return err
	}
	if descriptor.Mode != CapabilityRead && descriptor.Mode != CapabilityWrite {
		return &ValidationError{Field: "capability.mode", Problem: "must be read or write"}
	}
	if descriptor.Description == "" {
		return &ValidationError{Field: "capability.description", Problem: "is required"}
	}
	if err := ValidateTextIdentifier("capability.description", descriptor.Description, 512); err != nil {
		return err
	}
	if err := ValidateTextIdentifier("capability.since", strings.TrimSpace(descriptor.Since), 32); err != nil {
		return err
	}
	if len(descriptor.InputSchema) == 0 {
		return &ValidationError{Field: "capability.input_schema", Problem: "is required"}
	}
	if len(descriptor.InputSchema) > maxCapabilitySchemaBytes {
		return &ValidationError{Field: "capability.input_schema", Problem: "exceeds 65536 bytes"}
	}
	var schema map[string]interface{}
	if err := decodeCapabilityJSON(descriptor.InputSchema, &schema); err != nil {
		return &ValidationError{Field: "capability.input_schema", Problem: err.Error()}
	}
	if schema["type"] != "object" {
		return &ValidationError{Field: "capability.input_schema", Problem: "must describe an object"}
	}
	return nil
}

func ValidateCapabilityInvocation(invocation CapabilityInvocation) error {
	invocation.Name = strings.TrimSpace(invocation.Name)
	if err := validateCapabilityName(invocation.Name); err != nil {
		return err
	}
	if len(invocation.Arguments) == 0 {
		return nil
	}
	if len(invocation.Arguments) > MaxBodyBytes {
		return &ValidationError{Field: "capability.arguments", Problem: "exceeds maximum body size"}
	}
	var arguments map[string]interface{}
	if err := decodeCapabilityJSON(invocation.Arguments, &arguments); err != nil {
		return &ValidationError{Field: "capability.arguments", Problem: err.Error()}
	}
	if arguments == nil {
		return &ValidationError{Field: "capability.arguments", Problem: "must be an object"}
	}
	return nil
}

func validateCapabilityName(name string) error {
	if name == "" {
		return &ValidationError{Field: "capability.name", Problem: "is required"}
	}
	if len(name) > 128 {
		return &ValidationError{Field: "capability.name", Problem: "exceeds 128 bytes"}
	}
	segmentStart := true
	for _, character := range []byte(name) {
		if segmentStart {
			if character < 'a' || character > 'z' {
				return &ValidationError{Field: "capability.name", Problem: "must use lowercase dot-separated identifiers"}
			}
			segmentStart = false
			continue
		}
		switch {
		case character == '.':
			segmentStart = true
		case character >= 'a' && character <= 'z':
		case character >= '0' && character <= '9':
		case character == '-' || character == '_':
		default:
			return &ValidationError{Field: "capability.name", Problem: "must use lowercase dot-separated identifiers"}
		}
	}
	if segmentStart {
		return &ValidationError{Field: "capability.name", Problem: "must not end with a dot"}
	}
	return nil
}

func decodeCapabilityJSON(raw []byte, target interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}
