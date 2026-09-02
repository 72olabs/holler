package api

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/72olabs/holler/internal/bus"
)

const CapabilityBridgeCapability = "capability-bridge-v1"

const maxRegisteredCapabilities = 64

// CapabilityHandler runs inside hollerd with the authenticated connection
// identity. The daemon registry, not the caller, assigns its read/write mode.
type CapabilityHandler func(context.Context, Store, Identity, json.RawMessage) (interface{}, error)

type registeredCapability struct {
	descriptor bus.CapabilityDescriptor
	handler    CapabilityHandler
}

func WithCapability(descriptor bus.CapabilityDescriptor, handler CapabilityHandler) ServerOption {
	return func(server *Server) {
		if server.capabilityErr != nil {
			return
		}
		descriptor.Name = strings.TrimSpace(descriptor.Name)
		descriptor.Description = strings.TrimSpace(descriptor.Description)
		descriptor.Since = strings.TrimSpace(descriptor.Since)
		descriptor.InputSchema = append(json.RawMessage(nil), descriptor.InputSchema...)
		if err := bus.ValidateCapabilityDescriptor(descriptor); err != nil {
			server.capabilityErr = err
			return
		}
		if handler == nil {
			server.capabilityErr = fmt.Errorf("capability %q has no handler", descriptor.Name)
			return
		}
		if _, exists := server.capabilities[descriptor.Name]; exists {
			server.capabilityErr = fmt.Errorf("capability %q is registered more than once", descriptor.Name)
			return
		}
		if len(server.capabilities) >= maxRegisteredCapabilities {
			server.capabilityErr = fmt.Errorf("capability registry exceeds %d entries", maxRegisteredCapabilities)
			return
		}
		server.capabilities[descriptor.Name] = registeredCapability{descriptor: descriptor, handler: handler}
	}
}

func (s *Server) installDefaultCapabilities() {
	for _, registration := range defaultCapabilities() {
		WithCapability(registration.descriptor, registration.handler)(s)
	}
}

func (s *Server) listCapabilities() []bus.CapabilityDescriptor {
	result := make([]bus.CapabilityDescriptor, 0, len(s.capabilities))
	for _, registration := range s.capabilities {
		result = append(result, registration.descriptor)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })
	return result
}

func (s *Server) invokeCapability(ctx context.Context, identity Identity, expected bus.CapabilityMode, invocation bus.CapabilityInvocation) (interface{}, error) {
	invocation.Name = strings.TrimSpace(invocation.Name)
	if len(invocation.Arguments) == 0 {
		invocation.Arguments = json.RawMessage(`{}`)
	}
	if err := bus.ValidateCapabilityInvocation(invocation); err != nil {
		return nil, err
	}
	registration, exists := s.capabilities[invocation.Name]
	if !exists {
		return nil, &bus.ValidationError{Field: "capability", Problem: "is not supported: " + invocation.Name}
	}
	if registration.descriptor.Mode != expected {
		return nil, &bus.ValidationError{
			Field: "capability",
			Problem: fmt.Sprintf("%s is %s-only and cannot be invoked through the %s bridge",
				invocation.Name, registration.descriptor.Mode, expected),
		}
	}
	return registration.handler(ctx, s.store, identity, invocation.Arguments)
}

func defaultCapabilities() []registeredCapability {
	objectSchema := func(properties map[string]interface{}, required ...string) json.RawMessage {
		schema := map[string]interface{}{
			"type": "object", "properties": properties, "additionalProperties": false,
		}
		if len(required) > 0 {
			schema["required"] = required
		}
		raw, err := json.Marshal(schema)
		if err != nil {
			panic(err)
		}
		return raw
	}
	stringProperty := func(description string) map[string]interface{} {
		return map[string]interface{}{"type": "string", "description": description}
	}
	return []registeredCapability{
		{
			descriptor: bus.CapabilityDescriptor{
				Name: "alias.list", Mode: bus.CapabilityRead, Since: "0.6.1",
				Description: "List operator-approved aliases and their canonical actor targets.",
				InputSchema: objectSchema(map[string]interface{}{}),
			},
			handler: func(ctx context.Context, store Store, _ Identity, raw json.RawMessage) (interface{}, error) {
				if err := decodeStrict(raw, &struct{}{}); err != nil {
					return nil, err
				}
				aliases, err := store.ListAliases(ctx)
				if err != nil {
					return nil, err
				}
				return map[string]interface{}{"aliases": aliases}, nil
			},
		},
		{
			descriptor: bus.CapabilityDescriptor{
				Name: "alias.resolve", Mode: bus.CapabilityRead, Since: "0.6.1",
				Description: "Resolve one operator-approved alias to its canonical actor target.",
				InputSchema: objectSchema(map[string]interface{}{
					"alias": stringProperty("operator-approved alias"),
				}, "alias"),
			},
			handler: func(ctx context.Context, store Store, _ Identity, raw json.RawMessage) (interface{}, error) {
				var args struct {
					Alias string `json:"alias"`
				}
				if err := decodeStrict(raw, &args); err != nil {
					return nil, err
				}
				return store.ResolveAlias(ctx, args.Alias)
			},
		},
		{
			descriptor: bus.CapabilityDescriptor{
				Name: "alias.set", Mode: bus.CapabilityWrite, Since: "0.6.1",
				Description: "Create or repoint an actor alias after explicit user authorization.",
				InputSchema: objectSchema(map[string]interface{}{
					"alias":           stringProperty("human-friendly alias"),
					"actor":           stringProperty("existing canonical actor target"),
					"idempotency_key": stringProperty("stable key for this explicitly authorized change"),
				}, "alias", "actor", "idempotency_key"),
			},
			handler: func(ctx context.Context, store Store, identity Identity, raw json.RawMessage) (interface{}, error) {
				var args struct {
					Alias          string `json:"alias"`
					Actor          string `json:"actor"`
					IdempotencyKey string `json:"idempotency_key"`
				}
				if err := decodeStrict(raw, &args); err != nil {
					return nil, err
				}
				return store.SetAlias(ctx, bus.AliasSetRequest{
					Alias: args.Alias, Actor: args.Actor, UpdatedByActor: identity.Actor,
					UpdatedByRun: identity.RunID, ProjectID: identity.ProjectID, IdempotencyKey: args.IdempotencyKey,
				})
			},
		},
		{
			descriptor: bus.CapabilityDescriptor{
				Name: "alias.remove", Mode: bus.CapabilityWrite, Since: "0.6.1",
				Description: "Remove an actor alias after explicit user authorization.",
				InputSchema: objectSchema(map[string]interface{}{
					"alias":           stringProperty("alias to remove"),
					"idempotency_key": stringProperty("stable key for this explicitly authorized removal"),
				}, "alias", "idempotency_key"),
			},
			handler: func(ctx context.Context, store Store, identity Identity, raw json.RawMessage) (interface{}, error) {
				var args struct {
					Alias          string `json:"alias"`
					IdempotencyKey string `json:"idempotency_key"`
				}
				if err := decodeStrict(raw, &args); err != nil {
					return nil, err
				}
				return store.RemoveAlias(ctx, bus.AliasRemoveRequest{
					Alias: args.Alias, UpdatedByActor: identity.Actor, UpdatedByRun: identity.RunID,
					ProjectID: identity.ProjectID, IdempotencyKey: args.IdempotencyKey,
				})
			},
		},
	}
}
