package api

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/72olabs/holler/internal/buildinfo"
	"github.com/72olabs/holler/internal/bus"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestClientFallsBackToLegacyHelloWithoutBuildOrProject(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "ab-compat-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	listener, err := net.Listen("unix", filepath.Join(directory, "legacy.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		for attempt := 0; attempt < 3; attempt++ {
			connection, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			request, err := readRequest(connection)
			if err != nil {
				_ = connection.Close()
				done <- err
				return
			}
			var hello map[string]interface{}
			if err := json.Unmarshal(request.Args, &hello); err != nil {
				_ = connection.Close()
				done <- err
				return
			}
			_, hasBuild := hello["build"]
			_, hasProject := hello["project_id"]
			if attempt == 0 {
				if !hasBuild {
					done <- &testError{"first hello did not include build metadata"}
					_ = connection.Close()
					return
				}
				_ = writeResponse(connection, failure(request.ID, "bad_request", `json: unknown field "build"`, false))
				_ = connection.Close()
				continue
			}
			if hasBuild {
				done <- &testError{"legacy fallback still included build metadata"}
				_ = connection.Close()
				return
			}
			if attempt == 1 {
				if !hasProject {
					done <- &testError{"project-aware hello did not include project metadata"}
					_ = connection.Close()
					return
				}
				_ = writeResponse(connection, failure(request.ID, "bad_request", `json: unknown field "project_id"`, false))
				_ = connection.Close()
				continue
			}
			if hasProject {
				done <- &testError{"oldest legacy fallback still included project metadata"}
				_ = connection.Close()
				return
			}
			result, _ := json.Marshal(map[string]interface{}{
				"protocol": ProtocolVersion, "actor": "alice", "daemon": "hollerd/0.1",
			})
			_ = writeResponse(connection, Response{ID: request.ID, OK: true, Result: result})
			done <- nil
			return
		}
	}()

	client, err := Dial(context.Background(), listener.Addr().String(), Identity{
		Actor: "alice", RunID: "run-1", Client: "test", ProjectID: "coupon",
		Build: buildinfo.Info{Version: "test", Commit: "clean"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if client.ServerBuild().Commit != "" {
		t.Fatalf("legacy daemon unexpectedly reported a build: %+v", client.ServerBuild())
	}
	if _, err := client.Send(context.Background(), bus.SendRequest{
		Destinations: []bus.Route{{Kind: bus.RouteAlias, Value: "reviewer"}},
	}); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("typed route downgraded against legacy daemon: %v", err)
	}
	if _, err := client.ClaimAliasIfAbsent(context.Background(), bus.AliasClaimRequest{}); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("alias claim downgraded against legacy daemon: %v", err)
	}
}

func TestServerAcceptsLegacyHelloWithoutBuild(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "ab-legacy-server-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	db, err := store.Open(context.Background(), filepath.Join(directory, "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	listener, err := net.Listen("unix", filepath.Join(directory, "holler.sock"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- NewServer(db).Serve(ctx, listener) }()

	connection, err := net.Dial("unix", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(map[string]interface{}{
		"protocol": ProtocolVersion, "client": "legacy/0.1", "actor": "alice", "run_id": "run-1",
	})
	if err := writeRequest(connection, Request{ID: 1, Op: "hello", Args: args}); err != nil {
		t.Fatal(err)
	}
	response, err := readResponse(connection)
	if err != nil || !response.OK {
		t.Fatalf("legacy hello response=%+v err=%v", response, err)
	}
	_ = connection.Close()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestActorAllocationNeverDowngradesToLegacyHello(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "holler-allocation-compat-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	listener, err := net.Listen("unix", filepath.Join(directory, "legacy.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		for attempt := 0; attempt < 2; attempt++ {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				done <- acceptErr
				return
			}
			request, readErr := readRequest(connection)
			if readErr != nil {
				_ = connection.Close()
				done <- readErr
				return
			}
			var hello map[string]interface{}
			if unmarshalErr := json.Unmarshal(request.Args, &hello); unmarshalErr != nil {
				_ = connection.Close()
				done <- unmarshalErr
				return
			}
			if _, ok := hello["name_mode"]; !ok {
				_ = connection.Close()
				done <- &testError{"feature hello silently downgraded to legacy exact identity"}
				return
			}
			unknown := "build"
			if attempt == 1 {
				unknown = "capabilities"
			}
			_ = writeResponse(connection, failure(request.ID, "bad_request", `json: unknown field "`+unknown+`"`, false))
			_ = connection.Close()
		}
		done <- nil
	}()

	client, dialErr := Dial(context.Background(), listener.Addr().String(), Identity{
		Actor: "worker", RunID: "run-1", Client: "test", NameMode: "allocate",
		ContinuityHandles: []string{"launch:test:one"},
	})
	if client != nil {
		_ = client.Close()
	}
	if dialErr == nil {
		t.Fatal("actor allocation connected to a daemon without negotiated support")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type testError struct{ message string }

func (e *testError) Error() string { return e.message }
