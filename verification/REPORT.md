# Final verification report

This report records the completion evidence for the Alchemy Effect Agent goal. Raw command output is preserved beside this file.

## Commands and outcomes

| Check                | Command                                                                                                      | Outcome                                                                                                                                                                                                | Raw output                                                           |
| -------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------- |
| Frozen install       | `bun install --frozen-lockfile`                                                                              | 503 installs / 635 packages checked; no changes                                                                                                                                                        | [`install.log`](install.log), [`local-checks.log`](local-checks.log) |
| Type safety          | `bun run typecheck`                                                                                          | Exit 0 under strict TypeScript settings                                                                                                                                                                | [`local-checks.log`](local-checks.log)                               |
| Deterministic tests  | `bun test`                                                                                                   | 10 pass, 1 intentionally skipped live test, 0 fail, 61 assertions                                                                                                                                      | [`local-checks.log`](local-checks.log)                               |
| Production app       | `bun run build`                                                                                              | Vite client and SSR builds succeeded                                                                                                                                                                   | [`local-checks.log`](local-checks.log)                               |
| React audit          | `npx react-doctor@latest --verbose`                                                                          | 100/100, no issues                                                                                                                                                                                     | [`local-checks.log`](local-checks.log)                               |
| Live Cloudflare E2E  | `bun run test:e2e` with a non-default Alchemy env-auth profile populated from the local Wrangler OAuth token | Alchemy created the backend/AI/DO and website/service-binding resource groups; 11 live assertions passed through `AgentRpcs`; both groups were deleted successfully                                    | [`live-e2e.log`](live-e2e.log)                                       |
| Architecture leakage | Commands recorded in the linked log                                                                          | Only the permitted website proxy and Durable Object adapter contain raw Cloudflare access; no application `process.env`, raw Promise, unchecked cast, non-null assertion, or lowercase `any` was found | [`leakage-audit.log`](leakage-audit.log)                             |

No credential value is written to any artifact. The live command obtained the existing local Wrangler OAuth access token in a shell variable, selected the isolated `alchemy-agent-live` Alchemy profile with `CI=true`, and passed the token only in the child process environment.

## Requirement-to-evidence map

### Message Sending

- Protocol: `src/shared/agent-protocol.ts` defines `sendMessage` as `RpcSchema.Stream(AgentStreamEvent, AgentRpcError)`.
- Progressive server behavior: `src/server/agent-service-live.ts` consumes `LanguageModel.streamText`, maps every `text-delta` to `AgentStreamEvent.TextDelta`, and concatenates a persistence/continuation stream only after the provider stream completes.
- Visible client behavior: `src/client/components/message-composer.tsx` appends stream frames as received and renders the accumulating text in the `ASSISTANT · LIVE` region before refreshing durable history.
- Deterministic evidence: `test/agent-message-context.test.ts` observes two ordered `TextDelta` frames before `TurnCompleted` and verifies their concatenated text and persisted completion.
- Live evidence: the first and third live sends in `test/live.e2e.test.ts` traverse Website → private RPC Worker → session DO → Workers AI.

### Chat Compaction

- `src/server/chat-compaction.ts` computes a safe overlay range and moves its boundary away from tool interactions.
- `src/server/conversation.ts` assembles model context from durable context + latest summary + recent raw tail while leaving raw history unchanged.
- Three deterministic tests prove manual and threshold triggers, raw/tail preservation, context reduction, rehydration, and tool-safe boundaries.
- The live test verifies raw equality before/after, a persisted overlay, and fewer model-visible than raw messages.

### Tool calling

- `src/server/agent-toolkit.ts` defines the schema-backed deterministic `lookup_project_fact` Tool, Toolkit, and handler Layer.
- `src/server/agent-service-live.ts` uses a required non-streaming tool round when the explicit demo tool is requested, emits call/result events, then switches to native `streamText` for the progressive final answer. All paths retain the five-step bound.
- Deterministic tests verify call/result persistence, post-tool answer, handler output, and typed `ToolLoopLimitExceeded`.
- The live test observes the real Workers AI tool call/result and validates the protocol fact.

### Context

- `AgentContext` is part of `StoredSession`; the Durable Object adapter persists it.
- `assembleModelPrompt` injects system instructions and session memory into every model request.
- The Atom RPC UI reads and updates context.
- Deterministic and live tests both verify update, persistence, and model-visible recall.

### Shared RPC and serialization

- Browser, Website proxy, RPC Worker, and RPC Durable Object import the same `AgentRpcs` value.
- `test/agent-rpc-protocol.test.ts` deterministically inspects the group procedures, decodes/rejects branded payloads through the actual RPC payload schema, round-trips stream events and typed errors through their schemas, and round-trips the resulting RPC chunk frame through Effect's NDJSON serializer.
- The credentialed E2E test independently exercises the same group over HTTP/NDJSON.

## Leakage classification

The complete search output is in `leakage-audit.log`.

- `src/env.d.ts`: unavoidable framework type augmentation.
- `src/routes/rpc.ts`: the same-origin TanStack/Cloudflare composition boundary; its sole raw operation forwards the request over the private `BACKEND` service binding.
- `src/server/durable-object-session-store.ts`: the outbound Durable Object storage adapter; raw state is translated into the application-owned `SessionStore` and typed `AgentPersistenceError`.
- No inner application/domain module imports `cloudflare:workers`, accepts `Env`, or accesses a binding name.
- No application source directly reads `process.env` or creates raw Promises.
- `src/routeTree.ts` is a strict, hand-authored route tree. Its named option objects include a real typed `pendingComponent`, which bridges TanStack's public `FileRoute.update` type without assertions or suppression directives. TanStack's temporary generated tree is redirected to ignored `src/.tanstack/` and removed after each build.
- Searches cover all checked-in source and found no unchecked casts, `any` forms, non-null assertions, or TypeScript suppression directives.

## Resource cleanup

The successful live log ends with:

```text
[Website] deleted
[AgentBackend] deleted

Done: 2 succeeded
```

The local `.alchemy` deployment working directory was removed after verification; no deployed test stack was intentionally retained.
