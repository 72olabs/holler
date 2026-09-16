"""One arm plus one unsolicited managed attention turn for each real client."""


def run(context):
    f = context.managed()
    controller, a, b = "canary-controller", "canary-claude", "canary-codex"
    for actor in (controller, a, b):
        f.api(actor, "channel.list")
    channel = f.create(controller, [controller, a, b], "c11-attention")
    cid = channel["channel_id"]
    ids = []
    for client, actor in (("claude", a), ("codex", b)):
        context.check("c11-" + client + "-managed-wake")
        run_id = "c11-live-" + client
        ready = context.marker("C11_" + client.upper() + "_ARMED")
        woke = context.marker("C11_" + client.upper() + "_ACKED")
        with context.interactive(client, actor, run_id) as live:
            live.turn(
                'Initialize Holler. Use only Holler tools, no shell or file access. When a managed conversation '
                'notification arrives later, call holler_channel_claim for its message_id, process the synthetic '
                'message without replying, and holler_channel_ack with the returned lease_token. Do not use '
                'legacy bus_inbox/bus_ack or generic holler_write. After that successful awakened ACK finish with '
                + context.marker_instruction(woke) + '. For this initial arm turn only finish with '
                + context.marker_instruction(ready), ready)
            sent = live.wake(lambda: f.post(controller, cid, "c11-" + client, attention=[actor]), woke)
            ids.append(sent["message"]["message_id"])
        context.wait_for_session_end(actor, run_id, client)
        other = b if actor == a else a
        context.check("c11-non-attended-member-" + client)
        f.require(not any(d["message"]["message_id"] == ids[-1] for d in f.inbox(other)),
                  "non-attended member received a managed delivery")
    context.check("c11-terminal-no-duplicates")
    states = f.terminal(ids)
    targets = [d for d in states if (d["message_id"], d["recipient_actor"]) in ((ids[0], a), (ids[1], b))]
    others = [d for d in states if d not in targets]
    f.require(len(targets) == 2 and len(others) == 2, "wake recipient set mismatch")
    f.require(all(d["state"] == "absent" and d["attention_attempts"] == 0 and d["attention_adapters"] == 0 for d in others),
              "non-attended member has attention events")
    f.require(all(d["state"] == "acked" and d["attempt"] == 1 and d["claims"] == 1 and d["acks"] == 1 and d["attention_adapters"] >= 1 for d in targets),
              "wake produced missing or duplicate consumption")
    for actor, mid in zip((a, b), ids):
        f.require(not any(d["message"]["message_id"] == mid for d in f.inbox(actor)), "wake ACK lost on restart")
        f.api(actor, "channel.claim", {"message_id": mid}, error="no_message")
    f.assert_legacy_hidden(ids)
    return ["claude-managed-wake", "codex-managed-wake", "non-attended-member", "terminal-no-duplicates", "clients-ended"]
