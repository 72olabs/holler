package connector_test

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenCodePluginInjectsRegistrationFailureIntoSystemContext(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is unavailable")
	}
	version, err := exec.Command(node, "--version").Output()
	if err != nil {
		t.Skipf("cannot inspect node version: %v", err)
	}
	var major int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(version)), "v%d", &major); err != nil || major < 14 {
		t.Skipf("node 14 or newer is required by the OpenCode plugin behavior harness; found %q", strings.TrimSpace(string(version)))
	}
	plugin, err := filepath.Abs(filepath.Join("..", "..", "connectors", "marketplace", "plugins", "opencode-holler", "plugins", "holler.js"))
	if err != nil {
		t.Fatal(err)
	}
	script := `
globalThis.Bun = { spawn() { throw new Error("registration exploded") } };
const { HollerPlugin } = await import(` + "`file://${process.argv[1]}`" + `);
const hooks = await HollerPlugin({
  client: { app: { log: async () => {} } },
  serverUrl: new URL("http://127.0.0.1:4096"),
});
const output = { system: [] };
await hooks["experimental.chat.system.transform"]({ sessionID: "session-1" }, output);
if (output.system.length !== 1 || !output.system[0].includes("DEGRADED")) {
  throw new Error("degraded context was not injected: " + JSON.stringify(output));
}
`
	command := exec.Command(node, "--input-type=module", "--eval", script, plugin)
	combined, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("OpenCode plugin behavior: %v\n%s", err, strings.TrimSpace(string(combined)))
	}
}
