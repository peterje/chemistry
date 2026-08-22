import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { AgentPersistenceError } from "@chemistry/contracts/agent-protocol";
import {
  LegacyStoredSession,
  PersistedSession,
  SessionStore,
  migrateLegacyStoredSession,
  persistedSessionFromStored,
  storedSessionFromPersisted,
  type StoredSession,
} from "@chemistry/agent-runtime/session-store";

const decodePersistedSession = Schema.decodeUnknownResult(PersistedSession);
const decodeLegacyStoredSession = Schema.decodeUnknownResult(LegacyStoredSession);
const encodePersistedSession = Schema.encodeEffect(PersistedSession);

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

    const save = Effect.fn("SessionStore.save")(function* (session: StoredSession) {
      const encoded = yield* encodePersistedSession(persistedSessionFromStored(session)).pipe(
        Effect.mapError((cause) => persistenceError("encode", cause)),
      );
      yield* Effect.tryPromise({
        try: () => storage.put(keyFor(session.sessionId), encoded),
        catch: (cause) => persistenceError("save", cause),
      });
    });

    return SessionStore.of({
      getOrCreate: Effect.fn("SessionStore.getOrCreate")(function* (sessionId, initial) {
        const key = keyFor(sessionId);
        const existing = yield* Effect.tryPromise({
          try: () => storage.get<unknown>(key),
          catch: (cause) => persistenceError("get", cause),
        });
        if (existing === undefined) {
          yield* save(initial);
          return initial;
        }

        const current = decodePersistedSession(existing);
        if (Result.isSuccess(current)) return storedSessionFromPersisted(current.success);

        const legacy = decodeLegacyStoredSession(existing);
        if (Result.isFailure(legacy)) {
          return yield* persistenceError("decode", current.failure);
        }
        const migrated = migrateLegacyStoredSession(legacy.success);
        yield* save(migrated);
        return migrated;
      }),
      save,
    });
  }),
);
