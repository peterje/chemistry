import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type { SessionId } from "../shared/agent-protocol.ts";
import { RuntimeState } from "./runtime-state.ts";
import { RuntimePersistenceError, RuntimeStore, type RuntimeMutation } from "./runtime-store.ts";

const decodeRuntimeState = Schema.decodeUnknownResult(RuntimeState);

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
        if (Result.isFailure(decoded)) {
          return yield* persistenceError("decode", decoded.failure);
        }
        return decoded.success;
      }),
      transact: <A, E>(
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
                  if (Result.isFailure(decoded)) {
                    return Result.fail<E | RuntimePersistenceError>(
                      persistenceError("decode-transaction", decoded.failure),
                    );
                  }
                  const changed = await Effect.runPromise(Effect.result(mutation(decoded.success)));
                  if (Result.isSuccess(changed)) {
                    await transaction.put(keyFor(sessionId), changed.success.state);
                    return Result.succeed(changed.success.value);
                  }
                  return Result.fail<E | RuntimePersistenceError>(changed.failure);
                },
              ),
            catch: (cause) => persistenceError("transaction", cause),
          });
          if (Result.isFailure(outcome)) {
            return yield* Effect.fail(outcome.failure);
          }
          return outcome.success;
        }),
    });
  }),
);
