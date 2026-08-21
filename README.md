# Alchemy Effect Agent

A standalone Think-inspired agent demo built with Alchemy 2, Effect 4, and Cloudflare. The same Effect RPC contract is imported by the React browser client, the private Worker, and the per-session Durable Object.

The implemented surface is deliberately narrow:

- message sending over a typed NDJSON stream;
- durable system and memory context;
- schema-defined tool calling with a bounded agent loop;
- non-destructive chat compaction.

It does **not** depend on `@cloudflare/think`, the Cloudflare Agents SDK, the Vercel AI SDK, or a third-party model API.

## Architecture

```text
React + @effect/atom-react
  │  shared AgentRpcs schemas; NDJSON over /rpc
  ▼
TanStack Start Website Worker
  │  private BACKEND service binding
  ▼
Cloudflare.Workers.RpcWorker
  │  AgentSession.getByName(sessionId)
  ▼
Cloudflare.RpcDurableObject (one instance per session)
  ├─ AgentService (Effect Context service)
  ├─ SessionStore (Durable Object storage adapter)
  ├─ Effect LanguageModel (native Workers AI binding)
  └─ Effect Tool / Toolkit handlers
```

`src/shared/agent-protocol.ts` is the protocol source of truth. It contains branded IDs, all wire records and variants, typed errors, stream events, and `AgentRpcs`. No browser/server protocol types are duplicated.

Cloudflare values are confined to composition and adapter modules:

- `alchemy.run.ts`
- `src/routes/rpc.ts`
- `src/server/agent-backend.ts`
- `src/server/agent-durable-object.ts`
- `src/server/durable-object-session-store.ts`

The application service and conversation/compaction modules depend on Effect services and domain values, not `Env` or raw bindings.

## Think capability mapping

| Think capability | Effect / Alchemy implementation                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Message Sending  | `RpcSchema.Stream(AgentStreamEvent, AgentRpcError)` crosses browser → Website → RPC Worker → RPC Durable Object. `AgentServiceLive` consumes native `LanguageModel.streamText`, emits each assistant text delta immediately, persists role-correct response messages at round completion, and finishes with a durable completion frame. The composer renders the accumulating assistant answer under **ASSISTANT · LIVE** before the snapshot refresh. |
| Context          | `AgentContext` contains system instructions and mutable session memory. It is stored with the session and assembled into system messages on every inference request. The UI reads and updates it through Atom RPC.                                                                                                                                                                                                                                     |
| Tool calling     | `LookupProjectFact` is an Effect `Tool`; `AgentToolkit` supplies its schema; `AgentToolkitLive` supplies its deterministic handler. The application performs at most five model/tool rounds and returns `ToolLoopLimitExceeded` when the bound is exhausted.                                                                                                                                                                                           |
| Chat Compaction  | `buildCompactionPlan` selects a safe old range, never bisecting a tool call/result/final-answer interaction. Workers AI summarizes that range. The summary is persisted as an overlay while raw messages remain unchanged. Model prompts use the newest summary plus a six-message full-fidelity tail.                                                                                                                                                 |

### Why the transcript store is explicit

Alchemy includes `Cloudflare.AI.DurableObjectChatPersistence`, a good direct backing for Effect `Chat.layerPersisted` when one persisted `Prompt` is the complete record. This demo must preserve an immutable raw transcript **and** maintain replaceable model-visible summary overlays. Overwriting a single persisted `Chat.history` would destroy that distinction, so the authoritative store is the narrow `SessionStore` port implemented by `DurableObjectSessionStore`. Effect `Prompt`, `LanguageModel`, `Tool`, `Toolkit`, Services, Layers, Streams, RPC, and Schema remain native end to end.

## Requirements

- Bun 1.3+
- a Cloudflare account with Workers AI access
- Cloudflare authentication through `alchemy login` or `CLOUDFLARE_API_TOKEN`

No OpenAI, Anthropic, or other provider API key is used.

## Install

```bash
bun install
cp .env.example .env # optional model override
```

The project pins `alchemy@2.0.0-beta.72` and `effect@4.0.0-rc.110`, matching the APIs in the adjacent `../alchemy` reference checkout.

## Run

```bash
# Local Alchemy development environment
bun run dev

# Deploy Website, private RPC Worker, Durable Object, and Workers AI binding
bun run deploy

# Remove deployed resources
bun run destroy
```

The default model is:

```text
@cf/meta/llama-3.3-70b-instruct-fp8-fast
```

Override it with `WORKERS_AI_MODEL`. The value is read with Effect `Config` during Alchemy composition and bound into the runtime; application modules do not read environment variables directly.

## Demo walkthrough

1. Open `demo-session`, or enter another valid session name. Different names map to isolated Durable Objects.
2. Edit **Durable memory**, save it, then ask the agent to recall the fact.
3. Ask: `Call lookup_project_fact with topic protocol, then answer using its result.` The live strip and durable transcript show the call and result.
4. Send several longer turns. **Model context** shows raw versus model-visible message counts.
5. Choose **Compact eligible history**. Raw messages remain; the overlay count increases and the model-visible count falls.
6. Reload the page. Transcript, context, tool records, and compaction overlays rehydrate from Durable Object storage.

## Verification

Deterministic local tests use a real Effect `LanguageModel` service supplied by a fake provider Layer; they do not mock modules.

```bash
bun run check
```

This runs linting, typechecking, deterministic tests, and the production build. Lefthook runs the same verification before every commit after `bun install`.

The live test deploys the complete stack, calls it with the shared `AgentRpcs` client, verifies all four capabilities against native Workers AI and Durable Object storage, and destroys the stack in `afterAll`:

```bash
bun run test:e2e
```

The live test is skipped during ordinary `bun test`; it runs only when `RUN_LIVE_E2E=1`, which the script sets.

Auditable command output from the final verification is checked in under [`verification/`](verification/):

- [`install.log`](verification/install.log) — frozen dependency installation;
- [`local-checks.log`](verification/local-checks.log) — typecheck, deterministic tests (including RPC schema/NDJSON round-trips), production build, and React Doctor;
- [`live-e2e.log`](verification/live-e2e.log) — credentialed Alchemy create, 11 live assertions through the shared client, and successful deletion of both deployed resources;
- [`REPORT.md`](verification/REPORT.md) — requirement-to-evidence map and leakage-search classification.

## Typed failure surface

Expected failures cross RPC as schemas:

- `AgentPersistenceError`
- `AgentInferenceError`
- `AgentCompactionError`
- `ToolLoopLimitExceeded`

Raw storage, model-provider, and transport mechanics are classified at their owning adapter. Framework-level transport defects are converted to defects only in the private Worker proxy, where there is no truthful application recovery.

## Scope limits

This is capability parity for the four requested behaviors, not full Think compatibility. It intentionally omits resumable WebSockets, stream replay, cancellation/recovery incidents, FTS, branching, multi-agent orchestration, client tools, HITL approval, authentication, and production tenancy/retention policy.
