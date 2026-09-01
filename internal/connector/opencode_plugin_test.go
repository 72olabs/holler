package connector_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func openCodePluginTestNode(t *testing.T) string {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv("HOLLER_REQUIRE_OPENCODE_PLUGIN_TEST") != "" {
			t.Fatal("node 18 or newer is required by the OpenCode plugin behavior harness")
		}
		t.Skip("node is unavailable")
	}
	version, err := exec.Command(node, "--version").Output()
	if err != nil {
		if os.Getenv("HOLLER_REQUIRE_OPENCODE_PLUGIN_TEST") != "" {
			t.Fatalf("inspect node version: %v", err)
		}
		t.Skipf("cannot inspect node version: %v", err)
	}
	var major int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(version)), "v%d", &major); err != nil || major < 18 {
		message := fmt.Sprintf("node 18 or newer is required by the OpenCode plugin behavior harness; found %q", strings.TrimSpace(string(version)))
		if os.Getenv("HOLLER_REQUIRE_OPENCODE_PLUGIN_TEST") != "" {
			t.Fatal(message)
		}
		t.Skip(message)
	}
	return node
}

func openCodePluginTestPath(t *testing.T) string {
	t.Helper()
	plugin, err := filepath.Abs(filepath.Join("..", "..", "connectors", "marketplace", "plugins", "opencode-holler", "plugins", "holler.js"))
	if err != nil {
		t.Fatal(err)
	}
	return plugin
}

func runOpenCodePluginScript(t *testing.T, script string) {
	t.Helper()
	command := exec.Command(openCodePluginTestNode(t), "--input-type=module", "--eval", script, openCodePluginTestPath(t))
	combined, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("OpenCode plugin behavior: %v\n%s", err, strings.TrimSpace(string(combined)))
	}
}

func TestOpenCodePluginInjectsRegistrationFailureIntoSystemContext(t *testing.T) {
	script := `
const logs = [];
globalThis.Bun = { spawn() { throw new Error("registration exploded " + "x".repeat(1024)) } };
const { HollerPlugin } = await import(` + "`file://${process.argv[1]}`" + `);
const hooks = await HollerPlugin({
  client: { app: { log: async (entry) => logs.push(entry) } },
  serverUrl: new URL("http://127.0.0.1:4096"),
});
const output = { system: [] };
await hooks["experimental.chat.system.transform"]({ sessionID: "session-1" }, output);
if (output.system.length !== 1 || !output.system[0].includes("DEGRADED")) {
  throw new Error("degraded context was not injected: " + JSON.stringify(output));
}
const detail = logs[0]?.body?.extra?.detail || "";
if (detail.length > 512 || !detail.endsWith("...")) {
  throw new Error("degraded log detail was not bounded: " + detail.length);
}
`
	runOpenCodePluginScript(t, script)
}

func TestOpenCodePluginRegistersHydratesCompactsAndUnregisters(t *testing.T) {
	script := `
const calls = [];
const contexts = ["startup context", "compact context"];
globalThis.Bun = {
  spawn(argv, options) {
    const command = argv[1];
    const call = { argv, options, input: "" };
    calls.push(call);
    const stdout = command === "hook"
      ? JSON.stringify({ hookSpecificOutput: { additionalContext: contexts.shift() } })
      : "";
    return {
      stdin: {
        write(value) { call.input += String(value); },
        end() {},
      },
      stdout: new Blob([stdout]).stream(),
      stderr: new Blob([]).stream(),
      exited: Promise.resolve(0),
    };
  },
};
const { HollerPlugin } = await import(` + "`file://${process.argv[1]}`" + `);
const hooks = await HollerPlugin({
  client: { app: { log: async () => {} } },
  serverUrl: new URL("http://127.0.0.1:4096/"),
});

await hooks.event({ event: { type: "session.created", properties: { info: { id: "session-1" } } } });
const startup = { system: [] };
await hooks["experimental.chat.system.transform"]({ sessionID: "session-1" }, startup);
if (JSON.stringify(startup.system) !== JSON.stringify(["startup context"])) {
  throw new Error("startup context mismatch: " + JSON.stringify(startup));
}
const once = { system: [] };
await hooks["experimental.chat.system.transform"]({ sessionID: "session-1" }, once);
if (once.system.length !== 0) throw new Error("startup context was injected twice");

const compact = { context: [] };
await hooks["experimental.session.compacting"]({ sessionID: "session-1" }, compact);
if (JSON.stringify(compact.context) !== JSON.stringify(["compact context"])) {
  throw new Error("compact context mismatch: " + JSON.stringify(compact));
}
await hooks.event({ event: { type: "session.deleted", properties: { info: { id: "session-1" } } } });

if (calls.length !== 3 || calls[0].argv[1] !== "hook" || calls[1].argv[1] !== "hook" || calls[2].argv[1] !== "session-end") {
  throw new Error("lifecycle calls mismatch: " + JSON.stringify(calls.map((call) => call.argv)));
}
for (const call of calls) {
  if (JSON.parse(call.input).session_id !== "session-1") throw new Error("session input mismatch");
  if (call.options.env.HOLLER_OPENCODE_SERVER !== "http://127.0.0.1:4096") {
    throw new Error("server environment mismatch: " + call.options.env.HOLLER_OPENCODE_SERVER);
  }
}
`
	runOpenCodePluginScript(t, script)
}
