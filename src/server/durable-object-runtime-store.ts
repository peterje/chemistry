import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type { SessionId } from "../shared/agent-protocol.ts";
import { LegacyRuntimeStateV1, RuntimeState, migrateRuntimeStateV1 } from "./runtime-state.ts";
import { RuntimePersistenceError, RuntimeStore, type RuntimeMutation } from "./runtime-store.ts";

const decodeRuntimeState = Schema.decodeUnknownResult(RuntimeState);
const decodeLegacyRuntimeState = Schema.decodeUnknownResult(LegacyRuntimeStateV1);

const persistenceError = (operation: string, cause: unknown) =>
  new RuntimePersistenceError({
    operation,
    message: cause instanceof Error ? cause.message : String(cause),
  });

const keyFor = (sessionId: SessionId) => `agent-runtime:${sessionId}`;

/** Durable Object storage adapter for atomic versioned runtime transitions. */
export const DurableObjectRuntimeStore = Layer.effect(
  RuntimeStore,
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const storage = state.raw.storage;

    return RuntimeStore.of({
      load: Effect.fn("RuntimeStore.load")(function* (sessionId, initial) {
        const existing = yield* Effect.tryPromise({
          try: () => storage.get<unknown>(keyFor(sessionId)),
          catch: (cause) => persistenceError("load", cause),
        });
        if (existing === undefined) {
          yield* Effect.tryPromise({
            try: () => storage.put(keyFor(sessionId), initial),
            catch: (cause) => persistenceError("migrate", cause),
          });
          return initial;
        }
        const decoded = decodeRuntimeState(existing);
        if (Result.isSuccess(decoded)) return decoded.success;
        const legacy = decodeLegacyRuntimeState(existing);
        if (Result.isFailure(legacy)) {
          return yield* persistenceError("decode", decoded.failure);
        }
        const migrated = migrateRuntimeStateV1(legacy.success);
        yield* Effect.tryPromise({
          try: () => storage.put(keyFor(sessionId), migrated),
          catch: (cause) => persistenceError("migrate-v1", cause),
        });
        return migrated;
      }),
      transact: <A, E>(
        operation: string,
        sessionId: SessionId,
        initial: RuntimeState,
        mutation: (runtime: RuntimeState) => Effect.Effect<RuntimeMutation<A>, E>,
      ): Effect.Effect<A, E | RuntimePersistenceError> =>
        Effect.gen(function* () {
          const outcome: Result.Result<A, E | RuntimePersistenceError> = yield* Effect.tryPromise({
            try: () =>
              storage.transaction(
                async (transaction): Promise<Result.Result<A, E | RuntimePersistenceError>> => {
                  const existing = await transaction.get<unknown>(keyFor(sessionId));
                  const decoded =
                    existing === undefined ? Result.succeed(initial) : decodeRuntimeState(existing);
                  const current = Result.isSuccess(decoded)
                    ? decoded.success
                    : (() => {
                        const legacy = decodeLegacyRuntimeState(existing);
                        return Result.isSuccess(legacy)
                          ? migrateRuntimeStateV1(legacy.success)
                          : undefined;
                      })();
                  if (current === undefined) {
                    const cause = Result.isFailure(decoded)
                      ? decoded.failure
                      : "Runtime migration produced no state";
                    return Result.fail<E | RuntimePersistenceError>(
                      persistenceError(`${operation}:decode`, cause),
                    );
                  }
                  const changed = await Effect.runPromise(Effect.result(mutation(current)));
                  if (Result.isSuccess(changed)) {
                    await transaction.put(keyFor(sessionId), changed.success.state);
                    return Result.succeed(changed.success.value);
                  }
                  return Result.fail<E | RuntimePersistenceError>(changed.failure);
                },
              ),
            catch: (cause) => persistenceError(operation, cause),
          });
          if (Result.isFailure(outcome)) {
            return yield* Effect.fail(outcome.failure);
          }
          return outcome.success;
        }),
    });
  }),
);
