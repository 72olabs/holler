const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

function fixture() {
  const elements = new Map();
  function element() {
    return {
      hidden: false,
      value: "",
      textContent: "",
      open: false,
      children: [],
      classList: { add() {} },
      append(...nodes) {
        this.children.push(...nodes);
      },
      replaceChildren(...nodes) {
        this.children = nodes;
      },
      close() {
        this.open = false;
      },
      showModal() {
        this.open = true;
      },
      focus() {},
      querySelectorAll() {
        return [];
      },
    };
  }
  const document = {
    hidden: false,
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, element());
      return elements.get(id);
    },
    createElement: element,
  };
  const replies = [];
  const requests = [];
  const context = vm.createContext({
    document,
    crypto: { randomUUID: () => String(Math.random()) },
    window: { confirm: () => true },
    setInterval() {},
    Date,
    console,
    fetch: async (path, args) => {
      requests.push({ path, args: JSON.parse(args.body) });
      const reply = replies.shift();
      if (reply instanceof Error) throw reply;
      if (!reply) throw Error("missing test response");
      return {
        ok: reply.status === 200,
        status: reply.status,
        headers: { get: () => reply.retry || "" },
        json: async () => reply.body,
      };
    },
  });
  vm.runInContext(fs.readFileSync(__dirname + "/app.js", "utf8"), context);
  vm.runInContext(
    'session="session";human="human:test";current={channel_id:"one",last_seq:1,policy_revision:1,participants:[]};messages=[{message_id:"m"}];',
    context,
  );
  return {
    context,
    replies,
    requests,
    elements,
    run: (code) => vm.runInContext(code, context),
  };
}

test("poll retains cached conversation and backs off on rate limiting", async () => {
  const f = fixture();
  f.replies.push({
    status: 429,
    retry: "30",
    body: { error: "request not authorized" },
  });
  await f.run("poll()");
  assert.equal(f.run("current.channel_id"), "one");
  assert.equal(f.run("messages.length"), 1);
  assert.ok(f.run("backoffUntil>Date.now()"));
  const n = f.requests.length;
  await f.run("poll()");
  assert.equal(f.requests.length, n);
});
test("transient failures retain cached view, revocation clears it", async () => {
  const f = fixture();
  f.replies.push(new Error("temporary failure"));
  await f.run("poll()");
  assert.equal(f.run("current.channel_id"), "one");
  f.replies.push({ status: 404, body: { error: "conversation_denied" } });
  await f.run("poll()");
  assert.equal(f.run("current"), null);
  assert.equal(f.run("messages.length"), 0);
  assert.equal(f.elements.get("detail").hidden, true);
});
test("401 locks without retaining messages or session", async () => {
  const f = fixture();
  f.replies.push({ status: 401, body: { error: "request not authorized" } });
  await f.run("poll()");
  assert.equal(f.run("session"), "");
  assert.equal(f.run("messages.length"), 0);
  assert.equal(f.elements.get("workspace").hidden, true);
});
test("unchanged checkpoint skips history and response reload", async () => {
  const f = fixture();
  f.replies.push(
    { status: 200, body: [] },
    {
      status: 200,
      body: { channel_id: "one", last_seq: 1, policy_revision: 1 },
    },
  );
  await f.run("poll()");
  assert.deepEqual(
    f.requests.map((r) => r.args.operation),
    ["channel.list", "channel.get"],
  );
});
test("expired cursor is reloaded without losing the selected channel", async () => {
  const f = fixture();
  f.replies.push(
    { status: 409, body: { error: "cursor_expired" } },
    { status: 200, body: { messages: [], next_cursor: "" } },
  );
  await f.run("load()");
  assert.equal(f.run("current.channel_id"), "one");
  assert.equal(f.requests.length, 2);
});
