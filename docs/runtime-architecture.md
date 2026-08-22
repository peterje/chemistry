# Durable runtime architecture

Status: accepted implementation contract

This document defines the primitive runtime that `chemistry` implements before adding feature-level orchestration such as schedules, subagents, detached work, or human approval. It is a behavioral-parity target for the runtime guarantees in the checked-in Think reference, not a copy of Think's API or wire format.

## Guarantees and honest limits

The runtime guarantees:

- one ordering authority per named session Durable Object;
- durable admission before a turn can execute;
- append-before-publish stream events with a monotonic per-stream sequence;
- replay after a client high-water sequence with a race-free handoff to the live tail;
- idempotent submission admission and generation-fenced execution ownership;
- recovery of an interrupted logical turn from an explicit durable checkpoint;
- one durable terminal outcome per operation;
- bounded queues, frames, streams, retained streams, leases, retries, recovery work, alarms, and cleanup.

The runtime does **not** serialize a JavaScript stack or an arbitrary Effect fiber. A durable operation is a named program whose legal checkpoints are understood by its recovery handler. A recovered phase may run more than once. DO-local state transitions can be atomic; arbitrary external effects are at-least-once and require an idempotency key or application ledger.

A browser reconnect while the model iterator is still alive is byte/event-exact: persisted stream events are replayed and the same live producer continues. If an isolate or deploy destroys the Workers AI iterator, Workers AI exposes no stable upstream cursor. The runtime therefore persists the safe partial and starts a new model call from a transcript-level continuation checkpoint. It does not claim byte-exact provider continuation.

## Module seams

```text
Website WebSocket route (adapter)
  -> AgentBackend session router (adapter)
    -> AgentSession Durable Object (composition root)
      -> RuntimeProtocol (shared Effect Schemas)
      -> DurableExecution (application module)
          -> RuntimeStore (application-owned port)
              -> DurableObjectRuntimeStore (storage adapter)
      -> AgentService (existing model/context/tool/compaction module)
          -> SessionStore (existing transcript port)
      -> SocketHub (DO WebSocket adapter)

React
  -> ResumableAgentSocket (browser adapter)
  -> Effect RPC atoms (control/snapshots remain unchanged)
```

`DurableExecution` is the deep module. Callers admit a submission, execute/observe it, replay a stream, inspect runtime state, or recover eligible work. The module owns state transitions, sequencing, fencing, budgets, and terminalization. Callers do not manipulate persistence keys, sequence numbers, leases, or incident records.

The adapter audit found one existing persistence adapter, `DurableObjectSessionStore`. It remains the cohesive owner of transcript/context/compaction storage. Runtime records have a different lifecycle and write frequency, so they use a distinct `RuntimeStore` port and `DurableObjectRuntimeStore` adapter rather than widening transcript snapshots or allowing model writes to overwrite stream state. Runtime operation/stream terminal changes share one DO storage transaction. Provider calls never run inside a transaction. Transcript segments are persisted before the matching runtime terminal transition; deterministic message IDs and the recovery classifier converge the small transcript-before-runtime-terminal crash window instead of falsely claiming cross-module atomicity.

## Connection and resume state machine

```text
CONNECTING
  -> PROBE_SENT              server accepted a hibernatable socket
  -> REPLAYING               valid ResumeAck received
  -> LIVE                    backlog drained; live forwarding enabled
  -> CLOSED                  peer close or protocol violation

LIVE -> REPLAYING             reconnect with a high-water sequence
LIVE -> CLOSED                transport loss; producer is unaffected
```

On upgrade the server accepts the socket with the Hibernation API, stores a schema-versioned attachment, and sends `ResumeProbe`. The probe reports the current stream identity, latest durable sequence, operation state, and boot identity. The adapter registers the schema-encoded fixed `KeepAlive`/`KeepAliveAck` pair with Cloudflare's auto-response facility; those liveness frames keep the edge socket open without waking the object, while an ordinary typed `Ping` reaches the object and reports the handling boot ID. The client answers `ResumeAck(streamId, afterSequence)`. The server registers the connection as a live forwarder **before** reading the backlog, parks live events during the read, emits stored events in sequence, drains parked events through the same high-water filter, then sends `ResumeComplete` and switches to direct forwarding. Repeating an ACK is safe.

Socket attachments contain only parsed connection identity, session identity, protocol version, and last acknowledged stream cursor. Durable stream state never depends on an in-memory socket map. A hibernation wake decodes the attachment and reconstructs the forwarder on the first message.

Malformed frames produce a typed `ProtocolError` and close only when continuing would be unsafe. A stale stream cursor gets an explicit reset/probe response; it never mutates durable state.

## Durable stream state machine

```text
OPEN(sequence = -1)
  -> OPEN(sequence = n)       append event n, durably commit, then publish
  -> COMPLETED                terminal completion record
  -> FAILED                   terminal typed failure record
  -> INTERRUPTED              recoverable producer loss

INTERRUPTED -> OPEN           recovery claims a new generation
OPEN/terminal -> EXPIRED      bounded retention cleanup
```

A stream event is identified by `(streamId, sequence)`. Sequence starts at zero and increments by exactly one. An append transaction verifies the owning operation generation, verifies the expected next sequence, appends the event, and advances stream metadata before returning the event to a publisher. This is the persist-before-publish invariant.

Limits are part of the interface, not implementation trivia:

- maximum queued submissions: 16;
- maximum encoded client frame: 64 KiB;
- maximum persisted events per stream: 2,048;
- maximum encoded durable event payload: 60 KiB (leaving envelope headroom inside the 64 KiB frame);
- maximum cumulative encoded stream payload: 128 KiB;
- completed stream grace retention: 24 hours;
- maximum retained terminal streams: 8;
- maximum reconnect delay: 10 seconds;
- live handoff pending buffer: 256 events; overflow forces reconnect/replay rather than dropping durable events.

Cleanup runs in bounded batches and never deletes the active stream.

## Durable operation and turn state machine

```text
QUEUED(checkpoint=admitted)
  -> RUNNING(checkpoint=preparing)
  -> RUNNING(checkpoint=streaming)
  -> COMPLETED(checkpoint=terminal)
  -> FAILED(checkpoint=terminal)

RUNNING -> INTERRUPTED        producer disappeared or stall watchdog fired
INTERRUPTED -> RECOVERING     new generation/lease claimed
RECOVERING -> RUNNING         retry or transcript continuation starts
RECOVERING -> PARKED          explicit external interaction checkpoint
RECOVERING -> FAILED          budget exhausted or unrecoverable
PARKED -> QUEUED              a future feature supplies the awaited input
```

An operation record stores operation/submission/stream IDs, prompt and safe request snapshot, status, checkpoint, generation, attempt, progress/work counters, lease expiry, timestamps, and optional terminal reason. The submission ID is the idempotency key: duplicate admission returns the existing operation and never creates a second user turn.

The durable FIFO contains operation IDs. Claiming the head and setting the active operation happen atomically. There is at most one active turn per session. A fresh owner increments the generation and writes a bounded lease. Every append/checkpoint/terminal mutation supplies that generation; stale owners receive a typed fence error. A lease is ownership fencing, not a correctness timer: a live producer renews it only after durable progress.

The existing model service uses deterministic message IDs derived from the operation for runtime-driven turns. Re-running `preparing` does not append a duplicate user message. Continuation persists the reconstructed partial under the operation's deterministic assistant identity before making a new model request.

## Recovery incident state machine

```text
DETECTED
  -> SCHEDULED
  -> ATTEMPTING
  -> COMPLETED
  -> PARKED
  -> EXHAUSTED
  -> FAILED
```

Wake reconciliation runs before accepting a new turn. A nonterminal operation owned by an older boot or an expired lease is interrupted and classified:

- `pre-stream-retry`: admitted/preparing with no durable assistant event;
- `partial-continuation`: one or more safe durable response events exist;
- `terminal-noop`: transcript or stream already proves terminal completion;
- `parked`: explicit parked checkpoint;
- `unrecoverable`: corrupt snapshot, impossible transition, or terminal safety failure.

A recovery incident retains one identity across wakes. The engine records attempts, no-progress baseline/time, work baseline/consumption, OOM/reset strikes, generation, last schedule time, and terminal reason. Defaults are finite and configurable at the composition root:

- maximum no-progress attempts: 5;
- no-progress timeout: 2 minutes;
- maximum recovery work units: 1,024;
- maximum OOM/reset strikes: 3;
- lease duration: 30 seconds;
- alarm debounce: 1 second;
- stable-state timeout: 30 seconds;
- inference stall timeout: 45 seconds.

Forward progress resets the no-progress attempt window but not cumulative recovery work or OOM strikes. A stale alarm or generation cannot execute. Exhaustion writes one durable failure terminal and one replayable terminal frame. Alarm handling catches only classified memory-reset failures for the bounded strike circuit breaker; unrelated defects remain visible.

## Storage records and migration

Transcript data keeps its existing key and schema:

```text
agent-session:<sessionId> -> StoredSession v1
```

Runtime data is versioned separately:

```text
agent-runtime:<sessionId> -> RuntimeStateV1
runtime-alarm:<sessionId> -> next recovery/cleanup intent (inside RuntimeStateV1)
```

`RuntimeStateV1` contains:

- `version: 1`;
- bounded operation records;
- bounded FIFO operation IDs;
- active operation ID or null;
- bounded stream records and event arrays;
- active recovery incident or null;
- monotonic runtime generation;
- boot/wake metadata;
- next alarm intent or null.

Absence of `agent-runtime:*` is the migration from the existing release: construct an empty `RuntimeStateV1` without changing `agent-session:*`. Persisted runtime records are decoded with Effect Schema before application logic sees them. Unknown versions or malformed records fail as typed persistence/corruption errors; they are never silently reset.

## Failure semantics

| Failure                                 | Durable outcome                                          | Client outcome                                          |
| --------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------- |
| Browser disconnect                      | Producer and stream continue                             | Reconnect and replay after cursor                       |
| Hibernation while idle                  | No operation incident                                    | Attachment rehydrated; probe/replay                     |
| Isolate/deploy loss mid-turn            | Operation becomes interrupted; incident scheduled        | Replays partial, then recovering and continued terminal |
| Live inference stall                    | Watchdog interrupts owner and opens same incident policy | Recovering status; bounded retry/continue               |
| Duplicate submit                        | Existing operation returned                              | Same accepted IDs, no duplicate user message            |
| Stale owner append                      | Typed generation-fence failure                           | No stale event published                                |
| Queue/buffer/frame bound                | Typed capacity/protocol failure                          | Explicit error; durable state unchanged                 |
| Recovery exhaustion                     | One failed operation/stream terminal                     | Replayed terminal error; never indefinite loading       |
| Corrupt runtime record                  | Typed corruption failure, no reset                       | Explicit unavailable/error response                     |
| Arbitrary external side effect repeated | Application responsibility                               | Must use operation/phase idempotency key                |

## Think runtime parity matrix

| Think primitive                             | Chemistry runtime target                                            | Deliberate difference                                                 |
| ------------------------------------------- | ------------------------------------------------------------------- | --------------------------------------------------------------------- |
| Hibernatable WebSocket connection lifecycle | DO Hibernation API, attachments, typed probe/ACK                    | Own Effect Schema protocol, not Think/useAgentChat wire compatibility |
| `ResumableStream` persisted chunks          | Versioned `RuntimeStream` event log with high-water replay          | Typed domain events rather than AI SDK byte chunks                    |
| Resume handshake and terminal replay        | Register-before-drain handoff, sequence dedupe, replayable terminal | Effect-native browser adapter                                         |
| `runFiber` stash and wake detection         | Named durable operations with explicit checkpoints and snapshots    | No arbitrary fiber/stack serialization claim                          |
| Turn queue and submission dedupe            | Durable bounded FIFO and submission idempotency                     | FIFO only in this phase; no alternate concurrency policies            |
| Chat recovery incident engine               | Bounded incident with retry/continue/park/no-op classification      | Smaller host seam for one product/storage model                       |
| Progress/attempt/work/OOM bounds            | Finite configurable budgets and generation fencing                  | Conservative demo defaults                                            |
| Provider continuation                       | Persist partial and issue transcript-level Workers AI continuation  | No provider cursor/byte-exact resume                                  |
| HITL park                                   | Generic `PARKED` operation state                                    | Client tools/approval UX deferred                                     |
| Scheduled tasks/subagents/detached work     | DurableExecution is designed as their future substrate              | Features explicitly deferred                                          |

## Wake ordering

Every fetch, alarm, or WebSocket event enters the same readiness gate:

1. decode/migrate runtime state;
2. assign the current boot identity;
3. classify stale active ownership;
4. schedule or perform one bounded recovery pass;
5. restore the FIFO coordinator;
6. only then handle the external event.

The gate never performs an unbounded model turn inside constructor startup. It records/schedules recovery and lets a bounded background owner execute it through `waitUntil` or the alarm handler.

## Verification model

The deterministic adapter supports an explicit clock, boot ID, operation IDs, failure injection before/after every store transition, and recorded alarm intents. The transition matrix tests every crash point. Race tests register a live forwarder, append during backlog drain, and prove a strictly increasing unique sequence. Live verification records server boot identity so a hibernation/redeploy test can prove a wake or replacement occurred rather than merely reconnecting to the same in-memory producer. The credentialed replacement test uses a compile-time gate that is false in baseline source: its temporary fault build emits one deterministic partial, stalls, and aborts that isolate by alarm after the recovery build is deployed. This makes isolate loss repeatable without adding a remotely triggerable fault frame.
