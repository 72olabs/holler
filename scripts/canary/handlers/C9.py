"""Zero-model protocol and synthetic-human checks through the packaged daemon."""

from datetime import datetime, timedelta, timezone


def run(context):
    f = context.managed()
    a, b, outsider, human = "canary-claude", "canary-codex", "canary-stranger", "human:canary"
    for actor in (a, b, outsider):
        f.api(actor, "channel.list")
    context.check("c9-observer-read-only")
    f.supervise(a, linked=True, key="c9-link")
    dm = f.create(a, [a, b], "c9-agents", kind="dm")
    cid = dm["channel_id"]
    q = f.post(a, cid, "c9-question", attention=[b], respondent=b)["message"]
    qid = q["message_id"]
    view = f.human("channel.get", {"channel_id": cid})
    f.require(view["can_post"] is False and human in view["observers"], "observer rights incorrect")
    f.human("channel.history", {"channel_id": cid, "limit": 20})
    f.human("channel.post", {"channel_id": cid, "expected_policy_revision": view["policy_revision"],
                           "idempotency_key": "c9-forbidden", "body": {"text": "not authorized"}},
            error="conversation_denied")
    f.require(f.human("channel.inbox") == [], "observer received agent delivery")
    queued = f.inbox(b)
    f.require(len(queued) == 1 and queued[0]["attempt"] == 0 and queued[0]["state"] == "queued",
              "observation consumed pending work")
    preview = f.human("supervision.preflight", {"agent": b})
    f.human("supervision.commit", {"preview": preview, "human": "human:foreign", "idempotency_key": "c9-foreign"},
            error="conversation_denied")
    context.check("c9-legacy-isolation")
    f.assert_legacy_hidden([qid])
    queued_id = qid
    # Keep the source-side privacy checkpoint free of unrelated asynchronous
    # attention events from the queued-delivery test above.
    source = f.create(a, [a, b], "c9-reference-source")
    cid = source["channel_id"]
    qid = f.post(a, cid, "c9-source")["message"]["message_id"]
    f.post(a, cid, "c9-source-second")
    cursor = f.human("channel.history", {"channel_id": cid, "limit": 1})["next_cursor"]
    source_before = f.api(a, "channel.get", {"channel_id": cid})["last_seq"]
    context.check("c9-private-continuation")
    private = f.human("channel.continuation.preflight", {
        "source_message_id": qid, "create": {"project_id": "canary", "kind": "dm", "participants": [human, a]},
        "body": {"text": "synthetic private clarification"},
    })
    f.require(set(private["participants"]) == {human, a} and private["context_mode"] == "reference_only",
              "private audience preview incorrect")
    commit = {"preflight_token": private["preflight_token"], "idempotency_key": "c9-private"}
    pm = f.human("channel.continuation.commit", commit)["message"]
    f.require(f.human("channel.continuation.commit", commit)["message"]["message_id"] == pm["message_id"],
              "continuation retry duplicated message")
    f.api(b, "channel.message", {"message_id": pm["message_id"]}, error="conversation_denied")
    f.require(f.api(a, "channel.get", {"channel_id": cid})["last_seq"] == source_before,
              "private continuation emitted source-side metadata")
    context.check("c9-exact-audience-group")
    gp = f.human("channel.continuation.preflight", {
        "source_message_id": qid,
        "create": {"project_id": "canary", "kind": "named", "title": "c9-decision", "participants": [human, a, b]},
        "body": {"question": "synthetic decision", "context": "test only", "risks": "none", "tradeoffs": "test"},
        "respondent": b, "attention_targets": [b],
    })
    f.require(set(gp["participants"]) == {human, a, b}, "group audience preview incorrect")
    gm = f.human("channel.continuation.commit", {"preflight_token": gp["preflight_token"], "idempotency_key": "c9-group"})["message"]
    group = gm["channel_id"]
    context.check("c9-reference-redaction")
    rp = f.human("channel.continuation.preflight", {
        "source_message_id": qid,
        "create": {"project_id": "canary", "kind": "named", "title": "c9-redacted", "participants": [human, outsider]},
        "body": {"text": "new context only"},
    })
    rm = f.human("channel.continuation.commit", {"preflight_token": rp["preflight_token"], "idempotency_key": "c9-redacted"})["message"]
    redacted = f.api(outsider, "channel.message", {"message_id": rm["message_id"]})["references"]
    f.require(len(redacted) == 1 and redacted[0]["available"] is False and not redacted[0].get("source_message_id"),
              "inaccessible reference leaked source ID")
    context.check("c9-designated-response-decision")
    response = f.human("channel.responses", {"channel_id": group})[0]
    f.post(b, group, "c9-answer", response_to=response["request_id"], expected_response_revision=response["revision"])
    answered = f.human("channel.responses", {"channel_id": group})[0]
    f.require(answered["state"] == "answered", "designated response not answered")
    current = f.human("channel.get", {"channel_id": group})
    decision = f.human("channel.post", {"channel_id": group, "in_reply_to": answered["answer_id"],
        "expected_policy_revision": current["policy_revision"], "idempotency_key": "c9-decision",
        "body": {"kind": "studio.decision", "text": "proceed with synthetic work"}})["message"]
    f.require(decision["from_actor"] == human and decision["thread_id"] == gm["thread_id"], "human decision identity/thread incorrect")
    context.check("c9-view-restart")
    key = {"channel_id": group, "thread_id": gm["thread_id"]}
    state = f.human("channel.view.get", key)
    state.update(read_through_seq=decision["channel_seq"], manual_unread_from_seq=gm["channel_seq"],
                 snoozed_until=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(), muted=True, archived=True)
    saved = f.human("channel.view.update", state)
    f.restart()
    f.require(f.human("channel.view.get", key) == saved, "personal state lost across restart")
    f.human("channel.history", {"channel_id": cid, "cursor": cursor}, error="cursor_expired")
    cursor = f.human("channel.history", {"channel_id": cid, "limit": 1})["next_cursor"]
    f.require(any(d["message"]["message_id"] == queued_id and d["attempt"] == 0 for d in f.inbox(b)), "queued delivery lost on restart")
    context.check("c9-revocation-and-stranger-denial")
    f.api(outsider, "channel.get", {"channel_id": cid}, error="conversation_denied")
    f.supervise(a, linked=False, key="c9-unlink")
    f.human("channel.history", {"channel_id": cid}, error="conversation_denied")
    # Fresh authorization is checked even when an old pagination cursor exists.
    f.human("channel.history", {"channel_id": cid, "cursor": cursor}, error="conversation_denied")
    f.require(f.human("channel.get", {"channel_id": group})["can_post"], "independent human membership was revoked")
    # Clean up only these synthetic queued deliveries; keep later scenarios isolated.
    for delivery in f.inbox(b):
        mid = delivery["message"]["message_id"]
        claim = f.api(b, "channel.claim", {"message_id": mid})
        f.api(b, "channel.delivery", {"message_id": mid, "lease_token": claim["lease_token"], "action": "ack"})
    f.logout()
    return ["observer-read-only", "legacy-isolation", "private-continuation", "exact-audience-group",
            "reference-redaction", "designated-response-decision", "view-restart", "revocation-and-stranger-denial"]
