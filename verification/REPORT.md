# Chemistry monorepo and chat verification report

Status: implementation and required local/browser/live verification complete.

## Command evidence

| Check                          | Command                                                                    | Outcome                                                                                                                                      | Evidence                                                 |
| ------------------------------ | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| Frozen workspace install       | `bun install --frozen-lockfile`                                            | 616 installs / 785 packages checked; no lock changes                                                                                         | `verification/local-checks.log` and final command output |
| Formatting, lint, strict types | `bun run format`, `bun run lint`, `bun run typecheck`                      | Pass                                                                                                                                         | final command output                                     |
| Deterministic suite            | `bun test`                                                                 | 70 pass, 659 assertions, one intentionally skipped credentialed file                                                                         | `verification/local-checks.log` and final command output |
| Website production build       | `bun run build` followed by `bun run typecheck`                            | Client and SSR builds pass; post-build types pass                                                                                            | `verification/local-checks.log` and final command output |
| Browser suite                  | `bun run test:browser`                                                     | 4 pass: routing/history/reload/ACK, typed tools, responsive submission/navigation, query failures                                            | `verification/browser.log`                               |
| React audit                    | `npx react-doctor@latest --verbose --scope changed --include-untracked`    | 100/100, no issues                                                                                                                           | `verification/react-doctor.log`                          |
| Workspace/leakage audit        | checked source searches plus `bun test tools/workspace-boundaries.test.ts` | 360 boundary assertions pass; no root source/test tree, source bypass, inner Cloudflare leak, app env read, type escape, or old dashboard UI | `verification/leakage-audit.log`                         |
| Credentialed Cloudflare suite  | `ALCHEMY_PROFILE=default CI=true bun run test:e2e`                         | 3 pass, 53 assertions; all deployed resources deleted                                                                                        | `verification/live-e2e.log`                              |
| Whitespace                     | `git diff --check`                                                         | Pass                                                                                                                                         | final command output                                     |

## Requirement evidence

### Bun monorepo and package direction

- `apps/website` owns the TanStack/React client and its `/rpc` and `/ws` route adapters.
- `apps/backend` owns the Alchemy Worker, per-chat `AgentSession`, singleton `ChatCatalogObject`, Durable Object stores, and hibernatable WebSocket adapter.
- `packages/contracts`, `packages/client-runtime`, `packages/agent-runtime`, and `packages/chat-catalog` expose explicit package subpaths.
- Root `src` and root `test` no longer exist. Tests live with their owning app/package.
- `tools/workspace-boundaries.test.ts` enforces the acyclic dependency policy and rejects source-path interface bypasses.
- Root scripts perform local development, deploy/destroy, lint, strict types, all tests, browser tests, and the website production build. Local Alchemy was verified on website `:1337` and backend `:1338`.

### Durable conversation catalog

- `packages/chat-catalog/src/chat-catalog.ts` owns idempotent creation, first-user-prompt titles, deterministic recency order, and the 200-entry bound.
- `apps/backend/src/chat-catalog-durable-object.ts` persists the versioned catalog in one singleton Durable Object.
- Both RPC and WebSocket turn admission record server-side activity; catalog transport retries are bounded and cannot strand an already admitted durable turn.
- Unit tests cover creation/title/order/retention. The credentialed RPC test proves create/list and first-prompt title against Cloudflare.

### Canonical chat UI

- `/` durably creates a chat before navigating to `/chat/:chatId`.
- The responsive UI provides New chat, ordered/titled durable history, canonical links, history switching, transcript restoration, a centered transcript, sticky composer, Markdown, restrained tool details, streaming/recovery/error/loading states, keyboard sending, auto-scroll, and reduced-motion behavior.
- The architecture masthead, manual session selector, diagnostics, context editor, and compaction controls are absent from the normal UI; their backend contracts remain.
- Playwright uses the real React application and browser WebSocket transport. It verifies `StreamAck` traffic, non-empty native model text, reload durability, distinct new chats, ordered history, desktop collapse/reopen, tool events/final answer, recoverable transcript/history query failures, and mobile message submission plus history switching.
- React Doctor reports 100/100.

### Runtime preservation and live Cloudflare evidence

- Existing deterministic suites still prove append-before-publish, replay handoff, ACK bounds, FIFO/dedupe, leases/generation fencing, checkpoints, bounded recovery, migration, compaction, model context, and tools.
- The live suite proves native `@cf/zai-org/glm-5.2`, RPC controls, typed tools, catalog persistence, cursor replay, Hibernation API auto-responses, attachment wake in a new boot, stale cursor handling, and source-hash redeploy recovery.
- Final live hibernation evidence changed boot ID from `boot-41ee…` to `boot-52d6…` on the same socket.
- Final controlled isolate-loss evidence completed at generation 2, attempt 1, recovery work 104, with null terminal reason.
- The live suite deleted Website, AgentBackend, AI, AgentSession, ChatCatalogObject, and service-binding resources after completion.
