"use strict";
const $ = (id) => document.getElementById(id);
let session = "",
  human = "",
  scope = "",
  channels = [],
  current = null,
  messages = [],
  view = null,
  cursor = "",
  thread = "",
  previewToken = "",
  commitKey = "",
  mode = "",
  answer = null,
  busy = false;
let backoffUntil = 0,
  pendingPost = null;
const errorText = {
  audience_changed:
    "The audience changed. Refresh and review it before sending.",
  conversation_denied: "This conversation is no longer available to you.",
  cursor_expired: "History needs a refresh after a restart or access change.",
  preflight_expired:
    "This preview expired. Preview the audience and confirm again.",
  response_conflict:
    "This response request changed. Refresh it before answering.",
  idempotency_conflict:
    "This retry differs from the original message. Review it before sending.",
  invalid_request: "Some details are missing or invalid. Check the form.",
  capability_required: "This operation is not enabled in this Holler instance.",
};
const key = () => crypto.randomUUID();
function status(text) {
  $("status").textContent = text || "";
}
async function request(path, data) {
  const boundSession = session;
  const r = await fetch(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(session ? { Authorization: "Bearer " + session } : {}),
    },
    body: JSON.stringify(data || {}),
  });
  const result = await r.json();
  if (boundSession && boundSession !== session)
    throw Error("Your session changed; retry in the current workspace.");
  if (!r.ok) {
    if (r.status === 401 && path !== "/login") lock();
    const code = result.error || "request_failed";
    if (r.status === 429)
      backoffUntil =
        Date.now() +
        Math.max(1, Number(r.headers.get("Retry-After")) || 1) * 1000;
    const e = new Error(
      r.status === 429
        ? "Please wait a moment; the workspace will retry."
        : r.status === 401
          ? "Unlock the workspace to continue."
          : errorText[code] ||
            "The request could not complete. Refresh and try again.",
    );
    e.code = code;
    e.status = r.status;
    throw e;
  }
  return result;
}
const rpc = (operation, args = {}) =>
  request("/rpc", { operation, arguments: args });
function lock() {
  session = "";
  pendingPost = null;
  human = "";
  current = null;
  messages = [];
  channels = [];
  view = null;
  cursor = "";
  previewToken = "";
  answer = null;
  $("workspace").hidden = true;
  $("login").hidden = false;
  $("logout").hidden = true;
  $("detail").hidden = true;
  $("messages").replaceChildren();
  $("channels").replaceChildren();
  $("body").value = "";
  $("continuation-body").value = "";
  $("continuation").close();
  $("identity").textContent = "Local conversation workspace";
}
function safe(fn) {
  return async (event) => {
    event?.preventDefault();
    try {
      status("");
      await fn(event);
    } catch (e) {
      status(e.message);
    }
  };
}
function option(select, value, label) {
  const el = document.createElement("option");
  el.value = value;
  el.textContent = label;
  select.append(el);
}
function button(text, fn) {
  const b = document.createElement("button");
  b.type = "button";
  b.textContent = text;
  b.onclick = safe(fn);
  return b;
}
$("login-form").onsubmit = safe(async () => {
  const bearer = $("bearer").value;
  $("bearer").value = "";
  const r = await request("/login", { bearer });
  session = r.session;
  human = r.human;
  scope = r.scope;
  $("login").hidden = true;
  $("workspace").hidden = false;
  $("logout").hidden = false;
  $("admin").hidden = scope !== "observe+admin";
  $("identity").textContent = human;
  await refreshList();
});
$("logout").onclick = safe(async () => {
  try {
    await request("/logout");
  } finally {
    lock();
  }
});
async function refreshList() {
  channels = await rpc("channel.list");
  $("channels").replaceChildren();
  for (const c of channels) {
    const v = c.view;
    if (v.archived && !$("show-archived").checked) continue;
    const b = button(c.title || c.participants.join(" ↔ "), () =>
      open(c.channel_id),
    );
    if (current?.channel_id === c.channel_id) b.classList.add("selected");
    const meta = document.createElement("small");
    const unread =
      v.manual_unread_from_seq != null ||
      c.last_message_seq > v.read_through_seq;
    meta.textContent = [
      c.needs_response ? "Needs you" : c.can_post ? "Participant" : "Observing",
      unread ? "Unread" : "Read",
      v.snoozed_until && Date.parse(v.snoozed_until) > Date.now()
        ? "Snoozed"
        : "",
      v.muted ? "Muted" : "",
    ]
      .filter(Boolean)
      .join(" · ");
    b.append(meta);
    $("channels").append(b);
  }
  if (channels.length === 0)
    $("channels").textContent =
      "No conversations yet. Enroll observation or ask an agent to start a managed conversation.";
}
async function open(id) {
  current = await rpc("channel.get", { channel_id: id });
  view = await rpc("channel.view.get", { channel_id: id });
  messages = [];
  cursor = "";
  thread = "";
  answer = null;
  $("empty").hidden = true;
  $("detail").hidden = false;
  $("title").textContent = current.title || current.participants.join(" ↔ ");
  $("role").textContent = current.can_post
    ? "Participant"
    : "Read-only observer";
  $("audience").textContent =
    "Can post: " +
    current.participants.join(", ") +
    "\nRead-only observers: " +
    (current.observers.join(", ") || "None");
  $("history-floor").textContent = current.history_from
    ? "Your history starts at channel sequence " +
      current.history_from +
      ". Earlier messages remain private."
    : "Current audience shown above. Notifications do not change access.";
  $("compose").hidden = !current.can_post;
  $("join").hidden = current.can_post || current.kind === "dm";
  $("mute").textContent = view.muted ? "Unmute" : "Mute";
  $("archive").textContent = view.archived ? "Unarchive" : "Archive";
  await load();
  await responses();
  renderCompose();
}
async function load() {
  let page;
  try {
    page = await rpc("channel.history", {
      channel_id: current.channel_id,
      thread_id: thread,
      cursor,
      limit: 50,
    });
  } catch (e) {
    if (e.code !== "cursor_expired") throw e;
    cursor = "";
    messages = [];
    page = await rpc("channel.history", {
      channel_id: current.channel_id,
      thread_id: thread,
      limit: 50,
    });
  }
  messages.push(
    ...page.messages.filter(
      (m) => !messages.some((old) => old.message_id === m.message_id),
    ),
  );
  cursor = page.next_cursor || cursor;
  $("older").disabled = page.messages.length === 0;
  renderMessages();
}
function renderMessages() {
  $("messages").replaceChildren();
  for (const m of messages) {
    const article = document.createElement("article");
    article.className = "message";
    const meta = document.createElement("div");
    meta.className = "meta";
    meta.textContent =
      m.from_actor +
      " · " +
      new Date(m.created_at).toLocaleString() +
      " · " +
      m.thread_id;
    article.append(meta);
    if (m.in_reply_to) {
      const reply = document.createElement("p");
      reply.className = "secondary";
      reply.textContent = "Reply to " + m.in_reply_to;
      article.append(reply);
    }
    const body = m.body;
    if (
      body &&
      typeof body === "object" &&
      (body.question || body.context || body.risks || body.tradeoffs)
    ) {
      for (const field of [
        "question",
        "context",
        "risks",
        "tradeoffs",
        "recommendation",
        "text",
      ]) {
        if (body[field] != null) {
          const p = document.createElement("p"),
            strong = document.createElement("strong");
          strong.textContent = field + ": ";
          p.append(
            strong,
            typeof body[field] === "string"
              ? body[field]
              : JSON.stringify(body[field]),
          );
          article.append(p);
        }
      }
    } else {
      const p = document.createElement("p");
      p.textContent =
        typeof body === "string"
          ? body
          : body?.text || JSON.stringify(body, null, 2);
      article.append(p);
    }
    for (const ref of m.references || []) {
      const p = document.createElement("div");
      p.className = "reference";
      if (ref.available && ref.source_message_id) {
        p.append(
          button("View source reference", async () => {
            const source = await rpc("channel.message", {
              message_id: ref.source_message_id,
            });
            await open(source.channel_id);
          }),
        );
      } else {
        p.textContent = "Source context unavailable to you";
      }
      article.append(p);
    }
    $("messages").append(article);
  }
  const selected = $("threads").value;
  $("threads").replaceChildren();
  option($("threads"), "", "All threads");
  for (const t of new Set(messages.map((m) => m.thread_id)))
    option($("threads"), t, t);
  $("threads").value = selected || thread;
  renderCompose();
}
function renderCompose() {
  const old = $("reply-to").value;
  $("reply-to").replaceChildren();
  option($("reply-to"), "", "New thread");
  for (const m of messages)
    option(
      $("reply-to"),
      m.message_id,
      m.from_actor +
        ": " +
        (typeof m.body === "string"
          ? m.body
          : m.body?.text || m.message_id
        ).slice(0, 60),
    );
  $("reply-to").value = old;
  for (const name of ["notify", "respondent"]) {
    const sel = $(name),
      chosen = sel.value;
    sel.replaceChildren();
    option(
      sel,
      "",
      name === "notify" ? "No notification" : "No response required",
    );
    for (const a of current.participants.filter((a) => a !== human))
      option(sel, a, a);
    sel.value = chosen;
  }
}
async function responses() {
  const list = await rpc("channel.responses", {
    channel_id: current.channel_id,
  });
  $("requests").replaceChildren();
  for (const q of list) {
    const el = document.createElement("div");
    el.className = "request";
    el.textContent = q.requester + " asked " + q.respondent + " · " + q.state;
    if (q.state === "open" && q.respondent === human && current.can_post) {
      el.append(
        button("Answer", () => {
          answer = q;
          $("reply-to").value = q.message_id;
          $("body").focus();
          status("Your next message will answer this request.");
        }),
        button("Decline", async () => {
          await rpc("channel.response.resolve", {
            request_id: q.request_id,
            action: "decline",
            expected_revision: q.revision,
            idempotency_key: key(),
          });
          await responses();
        }),
      );
    }
    if (q.state === "open" && q.requester === human)
      el.append(
        button("Withdraw", async () => {
          await rpc("channel.response.resolve", {
            request_id: q.request_id,
            action: "withdraw",
            expected_revision: q.revision,
            idempotency_key: key(),
          });
          await responses();
        }),
      );
    $("requests").append(el);
  }
}
$("compose").onsubmit = safe(async () => {
  if (!current.can_post) return;
  const input = {
    channel_id: current.channel_id,
    expected_policy_revision: current.policy_revision,
    body: { text: $("body").value },
    in_reply_to: $("reply-to").value,
    attention_targets: $("notify").value ? [$("notify").value] : [],
    respondent: $("respondent").value,
  };
  if (answer) {
    input.response_to = answer.request_id;
    input.expected_response_revision = answer.revision;
    input.in_reply_to = answer.message_id;
  }
  const digest = JSON.stringify(input);
  if (!pendingPost || pendingPost.digest !== digest)
    pendingPost = { digest, key: key() };
  input.idempotency_key = pendingPost.key;
  await rpc("channel.post", input);
  pendingPost = null;
  $("body").value = "";
  answer = null;
  await open(current.channel_id);
});
async function updateView(change) {
  view = await rpc("channel.view.update", { ...view, ...change });
  await refreshList();
}
$("mark-read").onclick = safe(() =>
  updateView({
    read_through_seq: Math.max(
      view.read_through_seq,
      ...messages.map((m) => m.channel_seq),
    ),
    manual_unread_from_seq: null,
  }),
);
$("mark-unread").onclick = safe(() =>
  messages.length
    ? updateView({ manual_unread_from_seq: messages[0].channel_seq })
    : undefined,
);
$("snooze").onclick = safe(() =>
  updateView({ snoozed_until: new Date(Date.now() + 3600000).toISOString() }),
);
$("mute").onclick = safe(async () => {
  await updateView({ muted: !view.muted });
  $("mute").textContent = view.muted ? "Unmute" : "Mute";
});
$("archive").onclick = safe(async () => {
  await updateView({ archived: !view.archived });
  $("archive").textContent = view.archived ? "Unarchive" : "Archive";
});
$("threads").onchange = safe(async () => {
  thread = $("threads").value;
  view = await rpc("channel.view.get", {
    channel_id: current.channel_id,
    thread_id: thread,
  });
  messages = [];
  cursor = "";
  await load();
});
$("older").onclick = safe(load);
$("refresh").onclick = safe(refreshList);
$("show-archived").onchange = safe(refreshList);
function startContinuation(kind) {
  if (!messages.length) {
    status("Choose a visible message first.");
    return;
  }
  mode = kind;
  previewToken = "";
  commitKey = key();
  $("preview").hidden = true;
  $("confirm").hidden = true;
  $("preview-action").hidden = false;
  $("continuation-title").textContent =
    kind === "ask"
      ? "Ask an agent privately"
      : kind === "join"
        ? "Request admission from the channel creator"
        : "Discuss with selected actors";
  $("source").replaceChildren();
  for (const m of messages)
    option($("source"), m.message_id, m.from_actor + " · " + m.message_id);
  $("source").value = messages.at(-1).message_id;
  $("participants").replaceChildren();
  for (const a of [...new Set([human, ...current.participants])]) {
    const label = document.createElement("label"),
      input = document.createElement("input");
    input.type = "checkbox";
    input.value = a;
    input.checked =
      a === human ||
      kind === "discuss" ||
      (kind === "join" && a === current.created_by);
    input.disabled = a === human || kind === "join";
    label.append(input, a);
    $("participants").append(label);
  }
  $("continuation-body").value =
    kind === "join"
      ? "Please admit me to this conversation as myself, with join-forward history."
      : "";
  $("continuation").showModal();
}
$("ask").onclick = () => startContinuation("ask");
$("discuss").onclick = () => startContinuation("discuss");
$("join").onclick = () => startContinuation("join");
$("cancel").onclick = () => $("continuation").close();
$("continuation-form").oninput = () => {
  previewToken = "";
  $("confirm").hidden = true;
  $("preview-action").hidden = false;
  $("preview").hidden = true;
};
$("continuation-form").onsubmit = safe(async () => {
  const participants = [
    ...$("participants").querySelectorAll("input:checked"),
  ].map((i) => i.value);
  if (
    participants.length < 2 ||
    (mode !== "discuss" && participants.length !== 2)
  )
    throw Error(
      "Choose exactly one agent for a private DM, or at least one other actor for a discussion.",
    );
  const create = {
    project_id: current.project_id,
    kind: mode === "discuss" ? "named" : "dm",
    title: mode === "discuss" ? "Discussion with " + human : "",
    participants,
  };
  const preview = await rpc("channel.continuation.preflight", {
    source_message_id: $("source").value,
    create,
    body: { text: $("continuation-body").value },
    attention_targets: participants.filter((a) => a !== human),
  });
  previewToken = preview.preflight_token;
  $("preview").textContent =
    "Posting participants: " +
    preview.participants.join(", ") +
    "\nRead-only observers: " +
    (preview.observers.join(", ") || "None") +
    "\nContext: reference only. The source stays in its original channel.";
  $("preview").hidden = false;
  $("confirm").hidden = false;
  $("preview-action").hidden = true;
});
$("confirm").onclick = safe(async () => {
  if (!previewToken) return;
  const r = await rpc("channel.continuation.commit", {
    preflight_token: previewToken,
    idempotency_key: commitKey,
  });
  $("continuation").close();
  previewToken = "";
  await refreshList();
  await open(r.message.channel_id);
});
$("supervise").onsubmit = safe(async () => {
  const preview = await rpc("supervision.preflight", {
    agent: $("supervised-agent").value,
  });
  const count = Object.keys(preview.channel_revisions).length;
  if (
    !window.confirm(
      "Observe " +
        preview.agent +
        " as " +
        human +
        "? This changes " +
        count +
        " current channel audiences. Only messages after enrollment become readable; participants will see your observer identity.",
    )
  )
    return;
  await rpc("supervision.commit", { preview, human, idempotency_key: key() });
  await refreshList();
});
$("rotate").onclick = safe(async () => {
  if (
    !window.confirm(
      "Rotate the local bearer and invalidate every login session? You will need the new bearer from the credential file.",
    )
  )
    return;
  await request("/rotate");
  lock();
  status("Bearer rotated. Use the new bearer from your local credential file.");
});
async function poll() {
  if (
    !session ||
    busy ||
    document.hidden ||
    $("continuation").open ||
    Date.now() < backoffUntil
  )
    return;
  busy = true;
  try {
    await refreshList();
    if (current) {
      const latest = await rpc("channel.get", {
        channel_id: current.channel_id,
      });
      if (latest.policy_revision !== current.policy_revision) {
        await open(current.channel_id);
        status(
          "Audience changed. Review the refreshed audience before sending.",
        );
      } else if (latest.last_seq !== current.last_seq) {
        await load();
        await responses();
        current = latest;
      }
    }
  } catch (e) {
    if (e.code === "conversation_denied" && current) {
      messages = [];
      $("messages").replaceChildren();
      $("requests").replaceChildren();
      $("detail").hidden = true;
      current = null;
    }
    status(e.message);
  } finally {
    busy = false;
  }
}
setInterval(poll, 5000);
