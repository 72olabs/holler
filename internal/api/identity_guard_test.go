package api

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

func TestIdentityGuardsRejectBeforeWritingAPIFrame(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "check inbox actor", call: func(c *Client) error {
			_, err := c.CheckInbox(context.Background(), "stale-actor", 10)
			return err
		}},
		{name: "claim actor", call: func(c *Client) error {
			_, err := c.Claim(context.Background(), "stale-actor", "msg-1", time.Minute)
			return err
		}},
		{name: "ack actor", call: func(c *Client) error {
			return c.Ack(context.Background(), "stale-actor", "msg-1", "lease-1")
		}},
		{name: "extend actor", call: func(c *Client) error {
			_, err := c.Extend(context.Background(), "stale-actor", "msg-1", "lease-1", time.Minute)
			return err
		}},
		{name: "nack actor", call: func(c *Client) error {
			return c.Nack(context.Background(), "stale-actor", "msg-1", "lease-1", "retry", false)
		}},
		{name: "profile actor", call: func(c *Client) error {
			_, err := c.SetActorProfile(context.Background(), "stale-actor", "bound-run", "project", bus.ActorProfileRequest{})
			return err
		}},
		{name: "profile run", call: func(c *Client) error {
			_, err := c.SetActorProfile(context.Background(), "bound-actor", "stale-run", "project", bus.ActorProfileRequest{})
			return err
		}},
		{name: "heartbeat actor", call: func(c *Client) error {
			_, err := c.HeartbeatRegistrations(context.Background(), "stale-actor", "bound-run", time.Minute)
			return err
		}},
		{name: "heartbeat run", call: func(c *Client) error {
			_, err := c.HeartbeatRegistrations(context.Background(), "bound-actor", "stale-run", time.Minute)
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &writeCountingConn{}
			client := &Client{
				connection:    connection,
				reader:        bufio.NewReader(connection),
				identity:      Identity{Actor: "bound-actor", RunID: "bound-run"},
				helloIdentity: Identity{Actor: "bound-actor", RunID: "bound-run"},
			}
			err := test.call(client)
			if !errors.Is(err, bus.ErrIdentityRebound) {
				t.Fatalf("error = %v, want ErrIdentityRebound", err)
			}
			var validation *bus.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("error = %v, want ValidationError compatibility", err)
			}
			if writes := connection.writes.Load(); writes != 0 {
				t.Fatalf("API writes = %d, want zero before identity rejection", writes)
			}
		})
	}
}

type writeCountingConn struct{ writes atomic.Int64 }

func (c *writeCountingConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *writeCountingConn) Write(payload []byte) (int, error) {
	c.writes.Add(1)
	return len(payload), nil
}
func (c *writeCountingConn) Close() error                     { return nil }
func (c *writeCountingConn) LocalAddr() net.Addr              { return testAddr("local") }
func (c *writeCountingConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (c *writeCountingConn) SetDeadline(time.Time) error      { return nil }
func (c *writeCountingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *writeCountingConn) SetWriteDeadline(time.Time) error { return nil }

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }
