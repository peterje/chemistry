# Runtime design review

Reviewed: `docs/runtime-architecture.md`

Reference surfaces inspected:

- `agents/packages/think/src/think.ts` runtime overview, resume protocol, durable progress/recovery, and agent-tool replay paths;
- `agents/packages/agents/src/chat/resumable-stream.ts`;
- `agents/packages/agents/src/chat/resume-handshake.ts`;
- `agents/packages/agents/src/chat/recovery-engine.ts` and recovery incident modules;
- `agents/design/durable-streams-comparison.md`;
- `agents/design/rfc-chat-recovery-foundation.md`;
- `agents/design/rfc-chat-recovery-work-budget.md`;
- `agents/design/rfc-durable-object-lifecycle.md`;
- current Chemistry protocol, agent service, session store, RPC worker, and Durable Object composition.

## Contract review

- [x] Connection, stream, operation, turn, and incident states are explicit.
- [x] Live reconnect, socket hibernation, provider-loss recovery, and live stall recovery are distinct.
- [x] Persist-before-publish and register-before-backlog-drain are named invariants.
- [x] Sequence, idempotency, generation, and lease responsibilities have one owner.
- [x] Durable execution is checkpointed named work, not a serialized JavaScript stack.
- [x] External effects are honestly at-least-once and require idempotency.
- [x] Workers AI recovery is transcript continuation, not byte-exact upstream resume.
- [x] All queue/stream/frame/replay/recovery/alarm/retention paths have finite defaults or explicit policies.
- [x] The separate transcript and runtime adapters avoid stale model snapshots overwriting stream state.
- [x] Provider calls remain outside transactions; the transcript/runtime terminal crash window has deterministic convergence instead of a false atomicity claim.
- [x] Chat-only sessions migrate by absent runtime state; published v1 operations/queues/streams migrate explicitly to v2 snapshots and ledgers; malformed/unknown versions fail visibly.
- [x] RPC remains the control/snapshot interface while WebSocket becomes the live interface.
- [x] Above-high-water replay cursors close without advancing the gate; same-boot ownership is bounded by lease expiry.
- [x] Alarm claims require a due operation/generation/timestamp match; early intent is preserved and stale intent is consumed before wake rearms.
- [x] Operation kind, immutable input, consumed request snapshot, and bounded stable phase-effect keys are durable.
- [x] Scheduled tasks, subagents, detached work, and HITL remain feature-level non-goals.

## Adapter/module review

Existing adapter reuse was evaluated. `DurableObjectSessionStore` is retained unchanged in purpose because transcript/context/compaction records are cohesive and low-frequency. A new runtime store is justified because stream append frequency, operation fencing, alarm intent, and terminal cleanup change for a different reason. Both are composed only in the Durable Object root. No raw Cloudflare storage or socket type enters the domain/application modules.

The intended external seam is deliberately small: admit/execute, replay/inspect, recover. Storage transition helpers remain behind the `DurableExecution` implementation. The deletion test passes: deleting this module would spread sequencing, fencing, replay, recovery budgets, and terminalization into the WebSocket, RPC, and model callers.

## Review corrections applied

The first draft implied broader cross-record atomicity than the module split can provide. It now states the precise guarantee: runtime operation/stream terminal updates are transactional; transcript segments persist first; deterministic IDs and wake classification converge the narrow crash window. No network model call is held inside a Durable Object storage transaction.

## Verdict

The design is implementable against Alchemy's regular `Cloudflare.DurableObject` shape, which exposes `fetch`, `alarm`, `webSocketMessage`, and `webSocketClose`, plus `DurableObjectState.acceptWebSocket`. The current `RpcDurableObject` sugar exposes only `fetch`, so implementation must move the session runtime to the regular Durable Object composition form while continuing to serve the same Effect RPC HTTP handler from that `fetch` surface.
