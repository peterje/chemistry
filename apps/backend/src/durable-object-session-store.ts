import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { AgentPersistenceError } from "@chemistry/contracts/agent-protocol";
import { SessionStore, StoredSession } from "@chemistry/agent-runtime/session-store";

const decodeStoredSession = Schema.decodeUnknownEffect(StoredSession);

const persistenceError = (operation: string, cause: unknown) =>
  new AgentPersistenceError({
    operation,
    message: cause instanceof Error ? cause.message : String(cause),
  });

/** Durable Object storage adapter for one or more namespaced agent sessions. */
export const DurableObjectSessionStore = Layer.effect(
  SessionStore,
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const storage = state.raw.storage;
    const keyFor = (sessionId: string) => `agent-session:${sessionId}`;

    return SessionStore.of({
      getOrCreate: Effect.fn("SessionStore.getOrCreate")(function* (sessionId, initial) {
        const key = keyFor(sessionId);
        const existing = yield* Effect.tryPromise({
          try: () => storage.get<unknown>(key),
          catch: (cause) => persistenceError("get", cause),
        });
        if (existing === undefined) {
          yield* Effect.tryPromise({
            try: () => storage.put(key, initial),
            catch: (cause) => persistenceError("create", cause),
          });
          return initial;
        }
        return yield* decodeStoredSession(existing).pipe(
          Effect.mapError((cause) => persistenceError("decode", cause)),
        );
      }),
      save: Effect.fn("SessionStore.save")(function* (session) {
        yield* Effect.tryPromise({
          try: () => storage.put(keyFor(session.sessionId), session),
          catch: (cause) => persistenceError("save", cause),
        });
      }),
    });
  }),
);
