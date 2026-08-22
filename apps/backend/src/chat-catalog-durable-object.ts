import {
  ChatCatalogState,
  LegacyChatCatalogStateV1,
  createChat,
  emptyChatCatalog,
  listChats,
  migrateChatCatalogStateV1,
  touchChat,
} from "@chemistry/chat-catalog/chat-catalog";
import {
  AgentPersistenceError,
  ChatCatalogRpcs,
  type ChatSummary,
} from "@chemistry/contracts/agent-protocol";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
const STORAGE_KEY = "chat-catalog:v1";
const decodeState = Schema.decodeUnknownResult(ChatCatalogState);
const decodeLegacyState = Schema.decodeUnknownResult(LegacyChatCatalogStateV1);

const persistenceError = (operation: string, cause: unknown) =>
  new AgentPersistenceError({
    operation: `chat-catalog:${operation}`,
    message: cause instanceof Error ? cause.message : String(cause),
  });

/** Singleton Durable Object owning the bounded server-persisted conversation catalog. */
export default class ChatCatalogObject extends Cloudflare.DurableObject<ChatCatalogObject>()(
  "ChatCatalogObject",
  Effect.gen(function* () {
    const durableObject = yield* Cloudflare.DurableObjectState;
    const storage = durableObject.raw.storage;

    const mutate = Effect.fn("ChatCatalogObject.mutate")(function* (
      operation: string,
      transition: (
        state: ChatCatalogState,
        now: number,
      ) => readonly [ChatCatalogState, ChatSummary],
    ) {
      const now = yield* Clock.currentTimeMillis;
      const outcome = yield* Effect.tryPromise({
        try: () =>
          storage.transaction(async (transaction) => {
            const encoded = await transaction.get<unknown>(STORAGE_KEY);
            const decoded =
              encoded === undefined ? Result.succeed(emptyChatCatalog()) : decodeState(encoded);
            const current = Result.isSuccess(decoded)
              ? decoded.success
              : (() => {
                  const legacy = decodeLegacyState(encoded);
                  return Result.isSuccess(legacy)
                    ? migrateChatCatalogStateV1(legacy.success)
                    : undefined;
                })();
            if (current === undefined) {
              const cause = Result.isFailure(decoded)
                ? decoded.failure
                : "Catalog migration produced no state";
              return Result.fail(persistenceError(`${operation}:decode`, cause));
            }
            const [next, summary] = transition(current, now);
            await transaction.put(STORAGE_KEY, next);
            return Result.succeed(summary);
          }),
        catch: (cause) => persistenceError(operation, cause),
      });
      if (Result.isFailure(outcome)) return yield* outcome.failure;
      return outcome.success;
    });

    const load = Effect.fn("ChatCatalogObject.load")(function* () {
      const encoded = yield* Effect.tryPromise({
        try: () => storage.get<unknown>(STORAGE_KEY),
        catch: (cause) => persistenceError("list", cause),
      });
      if (encoded === undefined) return emptyChatCatalog();
      const decoded = decodeState(encoded);
      if (Result.isSuccess(decoded)) return decoded.success;
      const legacy = decodeLegacyState(encoded);
      if (Result.isFailure(legacy)) return yield* persistenceError("list:decode", decoded.failure);
      const migrated = migrateChatCatalogStateV1(legacy.success);
      yield* Effect.tryPromise({
        try: () => storage.put(STORAGE_KEY, migrated),
        catch: (cause) => persistenceError("list:migrate-v1", cause),
      });
      return migrated;
    });

    const handlers = ChatCatalogRpcs.toLayer({
      createChat: ({ sessionId }) =>
        mutate("create", (state, now) => createChat(state, sessionId, now)),
      listChats: () => load().pipe(Effect.map(listChats)),
      touchChat: ({ sessionId, prompt }) =>
        mutate("touch", (state, now) => touchChat(state, sessionId, prompt, now)),
    });

    const fetch = RpcServer.toHttpEffect(ChatCatalogRpcs).pipe(
      Effect.provide(handlers),
      Effect.provide(RpcSerialization.layerNdjson),
    );

    return Effect.succeed({ fetch });
  }),
) {}
