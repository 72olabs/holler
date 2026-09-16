"""Real writes plus per-recipient recovery/ACK using unchanged client policies."""


def run(context):
    f = context.managed()
    a, b, controller = "canary-claude", "canary-codex", "canary-controller"
    for actor in (a, b, controller):
        f.api(actor, "channel.list")
    context.check("c10-real-codex-create-post")
    marker = context.marker("C10_CREATED")
    context.run_codex(b, "c10-create", (
        'Use only Holler tools, no shell or file access. Discover channel.create and channel.post with holler_capabilities. '
        'Use holler_write channel.create with project_id=canary, kind=named, title=c10-real, '
        'participants=["canary-controller","canary-claude","canary-codex"], idempotency_key=c10-real. '
        'Then channel.post into that returned channel using its policy_revision as expected_policy_revision, '
        'idempotency_key=c10-opening, body={"text":"synthetic client opening"}, no attention targets. '
        'Do not use legacy bus_send. After both operations succeed, finish with '
        + context.marker_instruction(marker)), marker)
    context.wait_for_session_end(b, "c10-create", "codex")
    channels = [c for c in f.api(controller, "channel.list") if c["title"] == "c10-real"]
    f.require(len(channels) == 1, "real Codex create did not produce one channel")
    cid = channels[0]["channel_id"]
    opening = f.api(controller, "channel.history", {"channel_id": cid})["messages"]
    f.require(len(opening) == 1 and opening[0]["from_actor"] == b and opening[0]["from_run"] == "c10-create",
              "real Codex post correlation failed")
    sent = f.post(controller, cid, "c10-queued", attention=[a, b], respondent=b)["message"]
    mid = sent["message_id"]
    context.check("c10-restart-queued-identity")
    f.restart()
    for actor in (a, b):
        deliveries = [d for d in f.inbox(actor) if d["message"]["message_id"] == mid]
        f.require(len(deliveries) == 1 and deliveries[0]["state"] == "queued" and deliveries[0]["attempt"] == 0,
                  "per-recipient queued message lost or consumed on restart")
    f.assert_legacy_hidden([mid])
    context.check("c10-claude-narrow-consume")
    marker = context.marker("C10_CLAUDE_ACKED")
    context.run_claude(a, "c10-consume-claude", (
        'Use only Holler tools, no shell or file access. Call holler_channel_inbox, find message ' + mid +
        ', call holler_channel_claim with that message_id, process its synthetic context without replying, '
        'then holler_channel_ack with the returned lease_token. Never use bus_inbox or generic holler_write. '
        'Only after successful ACK finish with ' + context.marker_instruction(marker)), marker)
    context.wait_for_session_end(a, "c10-consume-claude", "claude")
    context.check("c10-recipient-independence")
    states = {d["recipient_actor"]: d for d in f.terminal([mid])}
    f.require(states[a]["state"] == "acked" and states[a]["attempt"] == 1 and states[a]["acks"] == 1,
              "Claude terminal ACK not proven")
    f.require(states[b]["state"] == "queued" and states[b]["attempt"] == 0,
              "Claude consumed Codex delivery")
    f.require(not any(d["message"]["message_id"] == mid for d in f.inbox(a)), "ACK not durable after restart")
    f.require(any(d["message"]["message_id"] == mid for d in f.inbox(b)), "other recipient lost after restart")
    context.check("c10-codex-designated-response-and-consume")
    marker = context.marker("C10_CODEX_ACKED")
    context.run_codex(b, "c10-consume-codex", (
        'Use only Holler tools, no shell or file access. Call holler_channel_inbox and holler_channel_claim for message ' + mid +
        '. This question designates you. Use holler_read channel.get and channel.responses for channel ' + cid +
        ' to get current revisions. Use holler_write channel.post with channel_id=' + cid +
        ', current expected_policy_revision, idempotency_key=c10-real-answer, body={"text":"synthetic recommendation"}, '
        'response_to=' + sent["response_request_id"] + ', expected_response_revision=1. '
        'Then holler_channel_ack the claimed message with its lease_token. Do not use legacy tools. '
        'Only after answer and ACK succeed finish with ' + context.marker_instruction(marker)), marker)
    context.wait_for_session_end(b, "c10-consume-codex", "codex")
    responses = f.api(controller, "channel.responses", {"channel_id": cid})
    f.require(len(responses) == 1 and responses[0]["state"] == "answered", "real designated response not answered")
    answer = f.api(controller, "channel.message", {"message_id": responses[0]["answer_id"]})
    f.require(answer["from_actor"] == b and answer["from_run"] == "c10-consume-codex" and answer["thread_id"] == sent["thread_id"],
              "real answer actor/run/thread correlation failed")
    context.check("c10-terminal-ack-exactly-once")
    states = f.terminal([mid])
    f.require(len(states) == 2 and all(d["state"] == "acked" and d["attempt"] == 1 and d["claims"] == 1 and d["acks"] == 1 for d in states),
              "managed delivery terminal state was not exactly one ACK per recipient")
    for actor in (a, b):
        f.require(not any(d["message"]["message_id"] == mid for d in f.inbox(actor)), "terminal inbox not empty")
        f.api(actor, "channel.claim", {"message_id": mid}, error="no_message")
    return ["restart-queued-identity", "claude-narrow-consume", "recipient-independence",
            "codex-designated-response", "codex-narrow-consume", "terminal-ack-exactly-once"]
