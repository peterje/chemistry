# Final code and spec audit

Fixed points: `f1b372f` (`Add anti-slop verification hooks`) for the original runtime diff and rejected publication `cfd9a8b` for the independent-audit remediation diff.

Reviewed surfaces:

- complete working-tree diff from `f1b372f`;
- active runtime objective and verification contract;
- `docs/runtime-architecture.md` and `verification/runtime-design-review.md`;
- critical admission/claim/append/settle/wake/recover/cleanup transitions;
- Durable Object transaction, RPC, alarm, Hibernation API, attachment, replay, and browser reconnect adapters;
- deterministic and credentialed live evidence;
- README claims, failure semantics, and scope exclusions.

## Standards axis

No unresolved finding.

- `bunx oxfmt --check .`, anti-slop `oxlint`, strict TypeScript, post-build typecheck, and `git diff --check` pass.
- No `any`, unchecked assertion, non-null assertion, blanket suppression, module mock, hardcoded credential, or application-layer environment read remains.
- Cloudflare and browser transport values stay in infrastructure/adapters. Runtime policy stays behind Effect Services and Layers.
- Exported runtime symbols have JSDoc.
- All loops, queues, frames, event payloads, cumulative stream bytes, streams, replay buffers, reconnection, attempts, work, memory-reset strikes, stalls, leases, alarms, retention, and cleanup are finite.
- The large `DurableExecution` implementation is cohesive around one changing concern—journal/checkpoint/ownership/recovery policy—and exposes a much smaller operation surface.

Judgement-call smells reviewed:

- Repeated tagged-union switches exist at protocol boundaries where exhaustive handling is clearer than speculative polymorphism.
- The compile-time live-chaos branch is deliberately narrow, defaults to `false`, has no remotely triggerable frame, and exists to produce repeatable Cloudflare isolate loss. The live test restores the source after each fault/recovery deployment.
- `AgentBackend.workersDev` is enabled only to verify the DO transport directly as well as through the Website binding; the README states this explicitly.

## Spec axis

No unresolved finding.

- Connection, stream, operation, FIFO, recovery, migration, and honest at-least-once semantics are documented.
- Shared Effect Schemas define all RPC and WebSocket frames; malformed/stale frames are bounded and safe, including above-high-water cursor rejection verified against live Cloudflare.
- Stream events commit before publication and use one monotonic sequence. Pre-ACK parking closes the probe/ACK race; replay and pending live events share one cursor.
- Admission, FIFO ownership, generation fencing, checkpointing, durable queue restoration, recovery incidents, terminalization, and cleanup are owned by `DurableExecution`.
- Workers AI continuation persists the reconstructed partial and makes a new model call. No byte-exact provider or arbitrary continuation claim is made.
- React reconnect/replay, reload reconstruction, diagnostics, prior context/tool/transcript/compaction behavior, and migration all have deterministic coverage.
- The final credentialed suite proves native Workers AI/RPC behavior, disconnect replay, same-socket hibernation wake with a changed boot, controlled mid-turn Worker replacement, generation-2/attempt-1 transcript continuation, coherent transcript/terminal state, and resource deletion.

## Corrections made during the first audit

1. Raised the browser's bounded retained-event window to the full 2,048-event server bound so a maximum-size replay cannot truncate early rendered output.
2. Disabled submission while a turn is visibly interrupted and awaiting recovery.
3. Replaced the RPC stream's success-only `onEnd` hook with an always-running finalizer so failed/stalled RPC turns still schedule durable recovery and queued-runner restoration.
4. Added 60 KiB per-event and 128 KiB cumulative stream persistence bounds, with deterministic no-persist/no-publish tests.
5. Split sent and acknowledged attachment cursors; stale/future ACKs now return a typed recoverable error and old ACKs cannot regress the cursor.

## Corrections made after the independent completion audit

The first independent completion audit rejected publication and identified five concrete gaps. Each was corrected rather than waived:

1. **Stale resume cursor:** `DurableExecution.replay` now raises `RuntimeCursorError` beyond the durable high-water mark; the WebSocket adapter sends a closing `stale-stream` frame without advancing the gate; the client clamps and prunes local state on the next probe.
2. **Same-boot lease loss:** wake now classifies a missing or expired lease as orphaned even when `ownerBootId` equals the current boot. A deterministic test injects this exact state.
3. **Alarm fencing/debounce:** recovery preparation now requires a due persisted alarm matching operation ID, generation, and scheduled timestamp. Early alarms preserve their intent; stale intents are consumed in a named transaction and wake rearms a current debounce. RPC readiness schedules rather than bypasses recovery, and active turns fence context/compaction mutation.
4. **Durable operation depth:** version-two operation records contain kind, immutable input, a consumed provider/model/context/history/compaction request snapshot, and a bounded generic phase-effect ledger. Stable effect keys govern admission, request capture, transcript partial persistence, inference attempts, and terminalization. Published v1 records migrate in place.
5. **Transition fault completeness:** every store transaction has a stable operation name. Named before/after commit tests now cover admission, claim, request snapshot, mark-streaming, append, settle, recovery scheduling, stale-alarm consumption, wake, recovery preparation, phase-effect begin/complete, transcript reconciliation, and cleanup, in addition to replay races, stale guards, limits, and migration.

## Final verification evidence

- Frozen install: 508 installs / 667 packages checked, no changes.
- Local: 60 passing tests, 279 assertions, one intentionally skipped live test, zero failures; format, lint, strict types, client/SSR build, and post-build types pass.
- Focused fault/recovery/replay matrix: 35 passing tests, 172 assertions, zero failures.
- React Doctor: 100/100, no issues.
- Live on the logged-in Personal account: 3 passing tests, 48 assertions; stale cursor rejection, reconnect replay, changed-boot hibernation wake, generation-2/attempt-1 recovery with 75 work units, and deletion of both resource groups.

## Honest residual limits

- External side effects outside the DO journal remain at-least-once and require their own idempotency keys.
- Workers AI continuation is transcript-level, not provider-token exact.
- The stream log is intentionally stored as a bounded DO record for this runtime demo, not as a production multi-gigabyte log service.
- Authentication, tenancy, schedules, subagents, detached tools, HITL UI, MCP, search, branching, and production quota policy remain out of scope.

Verdict: ready for the audited commit, push to `peterje/chemistry` `main`, remote SHA verification, and a new independent pi completion audit.
