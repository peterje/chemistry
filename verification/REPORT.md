# Runtime verification report

Status: implementation and verification complete. Publication remains subject to the independent code/spec audit and remote SHA checks recorded after the audited commit.

## Current command evidence

| Check                                                  | Command                                                                                                        | Current outcome                                                                                                                                                    | Evidence                                                                   |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------- |
| Frozen install                                         | `bun install --frozen-lockfile`                                                                                | 508 installs / 667 packages checked; no changes                                                                                                                    | [`install.log`](install.log)                                               |
| Format, lint, strict types, deterministic tests, build | `bunx oxfmt --check .`, `bun run lint`, `bun run typecheck`, `bun test`, `bun run build`, post-build typecheck | Passing: 60 tests, 279 assertions, one intentionally skipped live test, successful client/SSR builds, successful post-build typecheck                              | [`local-checks.log`](local-checks.log)                                     |
| React audit                                            | `npx react-doctor@latest --verbose --scope changed --include-untracked`                                        | 100/100, no issues                                                                                                                                                 | [`react-doctor.log`](react-doctor.log)                                     |
| Transition fault matrix                                | Focused runtime/fault/replay suite                                                                             | Before/after commit injection and all bounded recovery/race cases pass                                                                                             | [`runtime-fault-matrix.md`](runtime-fault-matrix.md)                       |
| Strictness/leakage                                     | Repository searches plus anti-slop lint                                                                        | Raw platform access is confined to adapters; no unchecked cast, non-null assertion, suppression, module mock, direct application env read, or hardcoded credential | [`leakage-audit.log`](leakage-audit.log)                                   |
| Live Cloudflare chaos                                  | `ALCHEMY_PROFILE=default CI=true bun run test:e2e` using the logged-in Personal account profile                | 3 tests and 48 assertions pass: RPC/Workers AI, cursor rejection/replay, hibernation wake, controlled mid-turn replacement/recovery; all resources deleted         | [`live-chaos.log`](live-chaos.log), [`live-profile.log`](live-profile.log) |

## Requirement-to-evidence map

### Runtime contract and semantics

- [`docs/runtime-architecture.md`](../docs/runtime-architecture.md) specifies connection, stream, operation, FIFO, incident, migration, limits, failure semantics, wake ordering, and the Think behavioral-parity boundary.
- [`runtime-design-review.md`](runtime-design-review.md) records the design audit and correction from broad atomicity language to precise DO-local transactions plus deterministic cross-record convergence.
- The documentation explicitly rejects arbitrary continuation serialization, byte-exact Workers AI resume after iterator loss, and exactly-once arbitrary external effects.

### Shared protocol, routing, and hibernatable transport

- `src/shared/agent-protocol.ts` is the sole schema source for session/submission/operation/stream/connection/boot identities; submit/accept, probe/ACK, sequence events, recovering, terminal, error, and ping/pong frames; and all RPC procedures.
- `src/shared/runtime-protocol.ts` bounds and validates JSON frames in both directions.
- `src/routes/ws.ts` and `agent-backend.ts` route each session to `AgentSession.getByName(sessionId)` while `/rpc` remains available.
- `runtime-websocket-adapter.ts` uses Alchemy's Hibernation API upgrade, versioned attachments, pre-ACK live-event parking, cursor replay, terminal replay, liveness boot IDs, bounded handoff buffering, safe invalid-frame handling, and wake scheduling.
- `runtime-protocol.test.ts`, `runtime-websocket-attachment.test.ts`, and `replay-handoff.test.ts` prove schema bounds, attachment rehydration, stale-version rejection, pre-ACK race handling, overlap deduplication, ordering, and overflow behavior. Replay rejects a cursor above the selected stream's durable high-water mark before the connection gate can advance; the browser clamps and prunes stale local state from the probe.
- The live suite proves actual Cloudflare acceptance and routing. Three typed Hibernation API auto-responses keep the same socket at the edge without waking the object; the next ordinary ping reports a different boot ID, proving attachment rehydration in a new isolate.

### Durable stream and execution core

- `DurableExecution` owns durable admission, bounded FIFO state, append-before-publish sequencing, operation kind/input/request snapshots, phase-effect ledgers, checkpoints, leases, generations, replay, terminals, incident classification, fenced alarm intent, and cleanup behind one deep Effect service.
- `RuntimeStore` and `TurnExecutor` are application ports; Durable Object and deterministic test Layers supply adapters.
- Runtime events and operation/stream terminal changes share one runtime transaction. Transcript segments persist first with deterministic IDs; recovery converges the transcript/runtime crash window.
- Wake exposes the oldest queued operation if its ephemeral runner disappeared. The adapter schedules it through the DO alarm path before later work can overtake it.
- `durable-execution.test.ts` covers persist-before-publish, cursor replay, duplicate submissions, FIFO, lost-runner restoration, session isolation, fencing, partial continuation, transcript-terminal convergence, parked work, stable-state and stall timeouts, attempt/work/no-progress/OOM bounds, and retention.

### Bounded recovery

- Wake trusts same-boot ownership only while its persisted lease is present and unexpired; otherwise it clears ownership, fences the abandoned runner's later mutations, and classifies pre-stream retry, partial continuation, terminal no-op, parked work, or corruption/unrecoverable work.
- Recovery claims a new generation, persists reconstructed partial output, and invokes a new native Workers AI call through `continueTurn`; stale owners are rejected.
- Exhaustion terminalizes once and remains replayable. Attempt, work, no-progress, memory-reset, lease, stall, alarm, queue, frame, stream, handoff, retention, and reconnect limits are finite.
- `runtime-fault-matrix.test.ts` has 13 transition tests injecting named failures immediately before and after admission, claim, request snapshot, mark-streaming, recovery scheduling, stale-alarm consumption, append, terminal, wake, recovery preparation, phase-effect begin/complete, reconciliation, and cleanup commits. [`runtime-fault-matrix.md`](runtime-fault-matrix.md) maps each family to its convergence assertion.

### React client and prior capabilities

- `ResumableAgentSocket` implements bounded exponential reconnect, typed resume, ACKs, gap-triggered replay, liveness, and cleanup.
- `runtime-client-state.ts` is a pure reducer for replay/live deduplication, cursor selection, reconnect actions, recovering/terminal diagnostics, and reload reconstruction.
- The UI displays connecting, replaying, live, recovering, completed, interrupted, failed, and disconnected states plus stream, sequence, operation/checkpoint, attempt, and terminal reason.
- Reducer and component tests prove cursor reuse, overlap dedupe, gap handling, full replay after reload, and visible diagnostics.
- Existing context, tools, immutable raw transcript, non-destructive compaction, streaming RPC compatibility, and session isolation remain covered by their original tests.
- React Doctor reports 100/100.

### Migration and strict boundaries

- Existing `agent-session:*` records remain unchanged. Missing runtime records create `RuntimeStateV2`; published `RuntimeStateV1` records migrate typed input, pending request snapshots, phase ledgers, queues, streams, and active ownership in place. Malformed or unknown versions fail rather than reset.
- `runtime-state-migration.test.ts` covers absence, v1-to-v2 operation migration, and unknown-version rejection.
- Raw Durable Object, Workers AI, fetch, and WebSocket values remain in composition/adapters. Inner runtime and conversation modules use Effect domain services and schemas.
- The anti-slop lint and leakage audit enforce no `any`, unchecked assertions, non-null assertions, blanket suppressions, module mocks, hardcoded credentials, or direct environment reads in application logic.

## Live Cloudflare evidence

The successful credentialed run in `live-chaos.log` proves:

1. native Workers AI context recall, typed tool execution, non-destructive compaction, and runtime snapshots through Effect RPC;
2. WebSocket session routing, invalid-frame closure, explicit above-high-water `stale-stream` rejection, disconnect-mid-stream cursor replay, complete replay idempotence, and retained RPC controls;
3. same-socket Hibernation API auto-responses across three idle windows followed by an ordinary ping with boot ID changing from `boot-71e6…` to `boot-9ab0…`, proving a new isolate decoded the stored attachment;
4. two source-hash Worker updates around a compile-time-only controlled fault: the fault deployment persists a deterministic partial then stalls, a DO alarm aborts the old isolate after the recovery deployment, and the new boot performs native Workers AI transcript continuation;
5. a completed redeploy terminal at generation 2, attempt 1, recovery work 75 (below the 1,024 bound), null reason, gap-free sequence, and exactly one user/assistant transcript pair;
6. Alchemy deletion of both website and backend/AI/DO resource groups (`Done: 2 succeeded`).

The successful run used Alchemy's logged-in `default` OAuth profile for the user's Personal Cloudflare account, recorded with its token redacted in `live-profile.log`; no credential value appears in any artifact. A prior obsolete explicit-account attempt was rate-limited before worker precreation and is retained in `live-chaos-audit-remediation-rate-limit.log`; all successful audit-remediation deployments and deletions used Personal. Other earlier failed attempts remain for audit history rather than being hidden.
