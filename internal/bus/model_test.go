package bus_test

import (
	"errors"
	"testing"

	"github.com/72olabs/holler/internal/bus"
)

func TestNormalizeSendRequestTypedRouteContract(t *testing.T) {
	base := bus.SendRequest{
		IdempotencyKey: "typed-route", ProjectID: "default", ChannelID: "direct",
		FromActor: "sender", FromRun: "run", Type: "MESSAGE", Body: []byte(`{"text":"hello"}`),
	}

	t.Run("alias", func(t *testing.T) {
		request := base
		request.Destinations = []bus.Route{{Kind: bus.RouteAlias, Value: " reviewer "}}
		normalized, err := bus.NormalizeSendRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		if len(normalized.Destinations) != 1 || normalized.Destinations[0] != (bus.Route{Kind: bus.RouteAlias, Value: "reviewer"}) {
			t.Fatalf("destinations = %+v", normalized.Destinations)
		}
	})

	t.Run("reply without recipient", func(t *testing.T) {
		request := base
		request.InReplyTo = "msg_parent"
		if _, err := bus.NormalizeSendRequest(request); err != nil {
			t.Fatal(err)
		}
	})

	for name, mutate := range map[string]func(*bus.SendRequest){
		"typed and legacy": func(request *bus.SendRequest) {
			request.Destinations = []bus.Route{{Kind: bus.RouteAlias, Value: "reviewer"}}
			request.ToActors = []string{"reviewer"}
		},
		"typed and reply": func(request *bus.SendRequest) {
			request.Destinations = []bus.Route{{Kind: bus.RouteActor, Value: "claude-a7f3c2"}}
			request.InReplyTo = "msg_parent"
		},
		"caller supplied reply route": func(request *bus.SendRequest) {
			request.Destinations = []bus.Route{{Kind: bus.RouteReply, Value: "msg_parent"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			mutate(&request)
			if _, err := bus.NormalizeSendRequest(request); !errors.Is(err, bus.ErrInvalid) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
