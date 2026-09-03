package attention

import (
	"context"
	"sync"

	"github.com/72olabs/holler/internal/bus"
)

type sessionKey struct {
	actor, runID, sessionID string
}

type waitResult struct {
	notice bus.AttentionNotice
	err    error
}

type waiterEntry struct {
	adapter string
	result  chan waitResult
}

// Broker connects daemon notification dispatch to one parked harness monitor
// per live session. It contains no durable state; the notification outbox is
// responsible for retrying until a monitor is present.
type Broker struct {
	mu      sync.Mutex
	waiters map[sessionKey]waiterEntry
	current map[string]sessionKey
}

func NewBroker() *Broker {
	return &Broker{
		waiters: make(map[sessionKey]waiterEntry), current: make(map[string]sessionKey),
	}
}

func (b *Broker) Attached(actor, runID, sessionID string) bool {
	key := sessionKey{actor: actor, runID: runID, sessionID: sessionID}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.current[actor] == key
}

// Attach makes this session the actor's one current in-memory attention
// presence. onTransition runs exactly once when the actor has no current
// presence or changes run/session; same-presence heartbeats do not run it.
// The callback runs under the broker lock and must not call back into Broker.
func (b *Broker) Attach(actor, runID, sessionID string, onTransition func() error) error {
	key := sessionKey{actor: actor, runID: runID, sessionID: sessionID}
	b.mu.Lock()
	previous, hasPrevious := b.current[actor]
	if hasPrevious && previous == key {
		b.mu.Unlock()
		return nil
	}
	if onTransition != nil {
		if err := onTransition(); err != nil {
			b.mu.Unlock()
			return err
		}
	}
	if hasPrevious {
		previousWaiter, waiting := b.waiters[previous]
		delete(b.waiters, previous)
		b.current[actor] = key
		b.mu.Unlock()
		if waiting {
			select {
			case previousWaiter.result <- waitResult{err: bus.ErrPresenceSuperseded}:
			default:
			}
		}
		return nil
	}
	b.current[actor] = key
	b.mu.Unlock()
	return nil
}

func (b *Broker) Wait(ctx context.Context, actor, runID, sessionID, adapter string) (bus.AttentionNotice, error) {
	key := sessionKey{actor: actor, runID: runID, sessionID: sessionID}
	result := make(chan waitResult, 1)
	b.mu.Lock()
	if current, ok := b.current[actor]; !ok {
		b.mu.Unlock()
		return bus.AttentionNotice{}, bus.ErrRegistrationExpired
	} else if current != key {
		b.mu.Unlock()
		return bus.AttentionNotice{}, bus.ErrPresenceSuperseded
	}
	if _, exists := b.waiters[key]; exists {
		b.mu.Unlock()
		return bus.AttentionNotice{}, bus.ErrAttentionWaiterBusy
	}
	b.waiters[key] = waiterEntry{adapter: adapter, result: result}
	b.mu.Unlock()

	remove := func() {
		b.mu.Lock()
		if current, ok := b.waiters[key]; ok && current.result == result {
			delete(b.waiters, key)
		}
		b.mu.Unlock()
	}

	select {
	case <-ctx.Done():
		remove()
		// A notification may have won the race immediately before cancellation.
		// Prefer that accepted delivery over reporting a timeout.
		select {
		case delivered := <-result:
			return delivered.notice, delivered.err
		default:
		}
		return bus.AttentionNotice{}, ctx.Err()
	case delivered := <-result:
		remove()
		return delivered.notice, delivered.err
	}
}

// Notify returns true only when the exact registered session has an active
// waiter and accepted the reference. The durable outbox retries false results.
func (b *Broker) Notify(registration bus.Registration, message bus.Message) (string, bool) {
	key := sessionKey{actor: registration.Actor, runID: registration.RunID, sessionID: registration.SessionID}
	b.mu.Lock()
	current, currentOK := b.current[registration.Actor]
	waiter, ok := b.waiters[key]
	if !currentOK || current != key || !ok || waiter.adapter != registration.AttentionMode {
		b.mu.Unlock()
		return "", false
	}
	notice := bus.AttentionNotice{
		MessageID: message.ID, ThreadID: message.ThreadID, FromActor: message.FromActor,
		Type: message.Type, DeliveryRequest: message.DeliveryRequest,
	}
	select {
	case waiter.result <- waitResult{notice: notice}:
		// Acceptance consumes this exact parked wait. Remove it while holding
		// the broker lock so a second notification cannot be accepted into the
		// channel after the first receiver wakes but before Wait cleans up.
		delete(b.waiters, key)
		b.mu.Unlock()
		return waiter.adapter, true
	default:
		b.mu.Unlock()
		return "", false
	}
}

// Cancel releases an exact waiter with a stable terminal outcome.
func (b *Broker) Cancel(actor, runID, sessionID string, outcome error) {
	key := sessionKey{actor: actor, runID: runID, sessionID: sessionID}
	b.mu.Lock()
	waiter, ok := b.waiters[key]
	delete(b.waiters, key)
	if b.current[actor] == key {
		delete(b.current, actor)
	}
	b.mu.Unlock()
	if !ok {
		return
	}
	select {
	case waiter.result <- waitResult{err: outcome}:
	default:
	}
}
