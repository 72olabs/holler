package lab

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed scenarios/*.yaml
var builtInScenarios embed.FS

type Scenario struct {
	Name         string        `yaml:"name" json:"name"`
	Description  string        `yaml:"description,omitempty" json:"description,omitempty"`
	Participants []Participant `yaml:"participants" json:"participants"`
	Steps        []Step        `yaml:"steps" json:"steps"`
	Assertions   []string      `yaml:"assert" json:"assert"`
}

type Participant struct {
	ID         string   `yaml:"id" json:"id"`
	Actor      string   `yaml:"actor" json:"actor"`
	Harness    string   `yaml:"harness" json:"harness"`
	NameMode   string   `yaml:"name_mode,omitempty" json:"name_mode,omitempty"`
	Continuity []string `yaml:"continuity,omitempty" json:"continuity,omitempty"`
}

type Step struct {
	Action         string `yaml:"action" json:"action"`
	Participant    string `yaml:"participant,omitempty" json:"participant,omitempty"`
	From           string `yaml:"from,omitempty" json:"from,omitempty"`
	To             string `yaml:"to,omitempty" json:"to,omitempty"`
	Route          string `yaml:"route,omitempty" json:"route,omitempty"`
	Body           string `yaml:"body,omitempty" json:"body,omitempty"`
	Type           string `yaml:"type,omitempty" json:"type,omitempty"`
	Message        string `yaml:"message,omitempty" json:"message,omitempty"`
	Reply          bool   `yaml:"reply,omitempty" json:"reply,omitempty"`
	ThreadID       string `yaml:"thread_id,omitempty" json:"thread_id,omitempty"`
	Claim          string `yaml:"claim,omitempty" json:"claim,omitempty"`
	SaveAs         string `yaml:"save_as,omitempty" json:"save_as,omitempty"`
	Alias          string `yaml:"alias,omitempty" json:"alias,omitempty"`
	Target         string `yaml:"target,omitempty" json:"target,omitempty"`
	Count          int    `yaml:"count,omitempty" json:"count,omitempty"`
	ContinuityFrom string `yaml:"continuity_from,omitempty" json:"continuity_from,omitempty"`
	Source         string `yaml:"source,omitempty" json:"source,omitempty"`
	ErrorContains  string `yaml:"error_contains,omitempty" json:"error_contains,omitempty"`
}

func BuiltInScenarioNames() ([]string, error) {
	entries, err := fs.ReadDir(builtInScenarios, "scenarios")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".yaml") {
			names = append(names, strings.TrimSuffix(entry.Name(), ".yaml"))
		}
	}
	sort.Strings(names)
	return names, nil
}

func LoadScenario(name, path string) (Scenario, []byte, error) {
	var raw []byte
	var err error
	if strings.TrimSpace(path) != "" {
		raw, err = os.ReadFile(path)
	} else {
		name = strings.TrimSpace(name)
		if name == "" {
			name = "direct-roundtrip"
		}
		raw, err = builtInScenarios.ReadFile("scenarios/" + name + ".yaml")
	}
	if err != nil {
		return Scenario{}, nil, err
	}
	var scenario Scenario
	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&scenario); err != nil {
		return Scenario{}, nil, fmt.Errorf("decode scenario: %w", err)
	}
	if err := scenario.Validate(); err != nil {
		return Scenario{}, nil, err
	}
	return scenario, raw, nil
}

func (s Scenario) Validate() error {
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("scenario name is required")
	}
	if len(s.Participants) == 0 {
		return fmt.Errorf("scenario %q has no participants", s.Name)
	}
	ids := make(map[string]struct{}, len(s.Participants))
	for _, participant := range s.Participants {
		if strings.TrimSpace(participant.ID) == "" || strings.TrimSpace(participant.Actor) == "" {
			return fmt.Errorf("scenario %q participant id and actor are required", s.Name)
		}
		if _, exists := ids[participant.ID]; exists {
			return fmt.Errorf("scenario %q repeats participant %q", s.Name, participant.ID)
		}
		ids[participant.ID] = struct{}{}
		switch participant.NameMode {
		case "", "exact", "allocate":
		default:
			return fmt.Errorf("participant %q has unsupported name_mode %q", participant.ID, participant.NameMode)
		}
	}
	validActions := map[string]bool{
		"start": true, "start_expect_error": true, "stop": true, "crash": true, "resume": true, "send": true,
		"claim": true, "ack": true, "alias_set": true, "adopt": true, "daemon_restart": true, "assert_inbox": true,
		"assert_actor_same": true, "assert_actor_distinct": true,
		"assert_original_recipient": true,
	}
	for index, step := range s.Steps {
		if !validActions[step.Action] {
			return fmt.Errorf("scenario %q step %d has unsupported action %q", s.Name, index+1, step.Action)
		}
		if step.Reply && (step.Action != "send" || strings.TrimSpace(step.Message) == "") {
			return fmt.Errorf("scenario %q step %d reply requires a send with message provenance", s.Name, index+1)
		}
		if step.Route != "" && step.Route != "actor" && step.Route != "alias" {
			return fmt.Errorf("scenario %q step %d has unsupported route %q", s.Name, index+1, step.Route)
		}
	}
	validAssertions := map[string]bool{
		"every_message_acked":    true,
		"durable_inboxes_empty":  true,
		"sandbox_paths_isolated": true,
		"orphan_processes_zero":  true,
		"socket_removed":         true,
	}
	for _, assertion := range s.Assertions {
		if !validAssertions[assertion] {
			return fmt.Errorf("scenario %q has unsupported assertion %q", s.Name, assertion)
		}
	}
	return nil
}
