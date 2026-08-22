# Chemistry: Effect-native Cloudflare agent runtime

Chemistry is a standalone agent runtime built with Alchemy 2, Effect 4, Cloudflare Durable Objects, native Workers AI, Effect RPC, and React. It demonstrates a deliberately small set of Think-inspired **runtime guarantees** without copying Think's wire format or public API.

The runtime now provides:

- typed hibernatable WebSockets per durable session;
- persist-before-publish event streams with cursor replay;
- race-free stored-replay-to-live handoff;
- checkpointed, generation-fenced durable operations;
- bounded FIFO turn admission and submission deduplication;
- bounded recovery after stalls, hibernation, isolate loss, or redeploy;
- transcript-level Workers AI continuation after iterator loss;
- React reconnect, replay, reconstruction, and runtime diagnostics;
- Effect RPC controls for transcript, context, compaction, streaming compatibility, and runtime snapshots.

It does **not** depend on `@cloudflare/think`, the Cloudflare Agents SDK, the Vercel AI SDK, or any third-party model API.

## Architecture

```text
React
  ├─ ResumableAgentSocket ─ typed JSON /ws ─┐
  └─ Effect Atom RPC ─── NDJSON /rpc ───────┤
                                             ▼
TanStack Start Website Worker
  └─ BACKEND service binding
       ▼
AgentBackend RpcWorker
  └─ AgentSession.getByName(sessionId)
       ▼
AgentSession Durable Object (one ordering authority per session)
  ├─ hibernatable WebSocket adapter + schema-versioned attachments
  ├─ Effect RPC handler
  ├─ DurableExecution
  │   └─ RuntimeStore → Durable Object storage
  └─ AgentService
      ├─ SessionStore → Durable Object storage
      ├─ native Workers AI LanguageModel
      └─ typed Effect Toolkit
```

Protocol definitions live in:

- [`src/shared/agent-protocol.ts`](src/shared/agent-protocol.ts) — branded identities, RPCs, events, runtime frames, and typed errors;
- [`src/shared/runtime-protocol.ts`](src/shared/runtime-protocol.ts) — bounded frame codecs.

The full state-machine and semantics contract is in [`docs/runtime-architecture.md`](docs/runtime-architecture.md).

## Runtime guarantees

### Connection and replay

A socket upgrade is accepted through Cloudflare's Hibernation API. The server stores a versioned attachment and sends `ResumeProbe`; the client responds with `ResumeAck(streamId, afterSequence)`. The server parks live events before waiting for the ACK, drains durable backlog, deduplicates overlap through one high-water cursor, drains parked events, then sends `ResumeComplete`. A typed fixed keepalive pair is registered with Cloudflare's WebSocket auto-response facility, so browser liveness traffic keeps the edge connection open without waking the Durable Object.

Events use a per-stream sequence beginning at zero. Each event is durably appended before it can be published. Cursor replay returns only events after the acknowledged sequence. Gaps force reconnect/replay rather than silent rendering.

### Durable execution and FIFO turns

`DurableExecution` hides operation journals, stream records, checkpoints, leases, generations, recovery incidents, alarms, and cleanup. Admission is durable and keyed by a client submission ID. Duplicate submissions converge on the same operation. One session Durable Object owns a bounded FIFO and executes at most one active turn.

Every producer mutation includes its generation. A wake from another boot—or an expired lease in the same boot—interrupts the old owner, and stale appends are rejected before publication. Operation records include an `agent-turn` kind, immutable input, a preparing-phase snapshot of model/context/history policy, and a bounded phase-effect ledger. Stable effect keys let transcript partial persistence converge across ambiguous commits while inference remains honestly at-least-once.

### Recovery

Recovery classifies interrupted work as pre-stream retry, partial continuation, terminal no-op, parked work, or unrecoverable work. Attempts, no-progress, total work, memory-reset strikes, leases, stalls, alarms, retained streams, and reconnects all have finite bounds. A recovery alarm must match the persisted operation, generation, and scheduled timestamp and must be due; early alarms preserve the intent, while stale alarms consume it without claiming work.

Workers AI does not expose a durable iterator cursor. After isolate loss, Chemistry reconstructs the safe partial and starts a new native Workers AI call from a transcript-level continuation prompt. This is **not** byte-exact provider resume. Durable phases are at-least-once; arbitrary external effects are not claimed to be exactly once.

### Existing agent capabilities

The runtime still supports:

- progressive native Workers AI responses;
- durable editable system prompt and memory;
- schema-defined `lookup_project_fact` tool calls with a five-step bound;
- immutable raw transcript history;
- non-destructive summary overlays and tool-safe compaction boundaries;
- session isolation by named Durable Object.

## Bounded defaults

| Resource                             |                 Default |
| ------------------------------------ | ----------------------: |
| Queued submissions                   |                      16 |
| Client frame / durable event payload |             64 / 60 KiB |
| Events / encoded bytes per stream    |         2,048 / 128 KiB |
| Replay handoff buffer                |              256 events |
| Retained terminal streams            |                       8 |
| Completed-stream grace               |                24 hours |
| Reconnect attempts / maximum delay   |          8 / 10 seconds |
| Recovery attempts / work units       |               5 / 1,024 |
| Phase-effect ledger entries          |                      32 |
| Memory-reset strikes                 |                       3 |
| Lease / stable-state timeout         | 30 seconds / 30 seconds |
| Inference stall watchdog             |              45 seconds |

## Requirements

- Bun 1.3+
- a Cloudflare account with Workers AI access
- Cloudflare authentication through `alchemy login` or `CLOUDFLARE_API_TOKEN`

No OpenAI, Anthropic, or other model-provider key is used. The default model is:

```text
@cf/zai-org/glm-5.2
```

The model is fixed at the composition root rather than accepted as an unchecked environment override. Changing it requires a code change and must pass the tool-contract browser and credentialed live suites; an empty semantic stream is a hard `AgentInferenceError`, never a fallback to a different request shape.

## Install and run

```bash
bun install
bun run dev       # local Alchemy environment
bun run deploy    # Cloudflare deployment
bun run destroy   # remove deployed resources
```

Local development pins the Website to `http://localhost:1337` and `AgentBackend` to `http://localhost:1338`. Because the local Website service-binding proxy cannot tunnel WebSocket upgrades, the browser adapter connects directly to port 1338 on localhost; deployed builds remain same-origin through `/ws`.

In the UI, choose a session, update durable memory, and submit a turn. The runtime panel displays connection state, stream ID, sequence, operation/checkpoint, recovery attempt, and terminal reason. Reload or disconnect during a turn to exercise cursor replay.

## Verification

Local verification:

```bash
bun install --frozen-lockfile
bun run check
bunx playwright install chromium # first browser-test run only
bun run test:browser
npx react-doctor@latest --verbose --scope changed --include-untracked
```

The deterministic suite covers protocol bounds, persist-before-publish sequencing, replay races, cursor deduplication, durable FIFO ordering, duplicate admission, stale-owner fencing, partial continuation, transcript/runtime crash windows, stalls, OOM/reset and recovery budgets, terminal replay, retention, migration, client reload reconstruction, and prior message/context/tool/compaction behavior. The Playwright suite starts or reuses `bun run dev`, sends `hi` through the actual React UI, requires non-empty model text, observes browser WebSocket frames, requires live `StreamAck` traffic with no protocol or console transport error, waits for a completed terminal, and verifies the durable transcript. A second browser test requires the configured model to complete `lookup_project_fact` with typed `ToolCall` and `ToolResult` events plus a non-empty final answer.

Credentialed Cloudflare chaos verification:

```bash
alchemy login
bun run test:e2e
```

The live suite deploys with Alchemy, exercises native Workers AI and RPC controls, ACKs every replay/live event with the browser protocol, disconnects mid-stream and resumes from a cursor, proves same-socket attachment wake after Hibernation API auto-responses, and performs two Worker source-hash updates around a controlled compile-time fault deployment. That fault deployment persists a deterministic partial and aborts its old isolate by alarm; the recovery deployment must continue it through a new native Workers AI call under a new generation. The baseline gate is `false`, and the test restores the source file after each deployment. The suite verifies one coherent terminal/transcript and destroys the stack in `afterAll`.

Evidence is preserved in [`verification/`](verification/):

- [`install.log`](verification/install.log)
- [`local-checks.log`](verification/local-checks.log)
- [`react-doctor.log`](verification/react-doctor.log)
- [`runtime-fault-matrix.md`](verification/runtime-fault-matrix.md)
- [`runtime-design-review.md`](verification/runtime-design-review.md)
- [`leakage-audit.log`](verification/leakage-audit.log)
- [`live-chaos.log`](verification/live-chaos.log)
- [`REPORT.md`](verification/REPORT.md)

## Cloudflare composition boundaries

Raw Cloudflare values and transport bridges are confined to infrastructure and adapter modules: `alchemy.run.ts`, route proxies, `agent-backend.ts`, `agent-durable-object.ts`, both Durable Object stores, and `runtime-websocket-adapter.ts`. Browser-native WebSocket handling is confined to `resumable-agent-socket.ts`. Domain and application modules depend on Effect Services, Layers, Schemas, Streams, and typed errors. `AgentBackend` has a workers.dev URL solely so the credentialed suite can test the DO transport directly as well as through the Website service binding.

## Scope limits

This is behavioral parity for the core runtime primitives, not full Think compatibility. It intentionally omits scheduled tasks, subagents, detached tools, MCP, channels, voice, search, branching, client tools/HITL beyond a generic parked checkpoint, authentication, production tenancy/quotas, and Cloudflare Workflows as the execution substrate.
