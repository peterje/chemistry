import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import type { SessionId } from "../../src/shared/agent-protocol.ts";
import type { RuntimeState } from "../../src/server/runtime-state.ts";
import {
  RuntimePersistenceError,
  RuntimeStore,
  type RuntimeMutation,
  type RuntimeStoreOperations,
} from "../../src/server/runtime-store.ts";

/** Deterministic controls exposed by the runtime-store test adapter. */
export interface RuntimeStoreTestOperations extends RuntimeStoreOperations {
  /** Inspect the current state for one session. */
  readonly inspect: (sessionId: SessionId) => Effect.Effect<Option.Option<RuntimeState>>;
  /** Replace one session state to inject a deterministic wake or crash fixture. */
  readonly replace: (state: RuntimeState) => Effect.Effect<void>;
  /** Inject one failure at the next matching named transaction. */
  readonly failOperation: (operation: string, position: "before" | "after") => Effect.Effect<void>;
}

/** Test-only service backed by the same object as {@link RuntimeStore}. */
export class RuntimeStoreTest extends Context.Service<
  RuntimeStoreTest,
  RuntimeStoreTestOperations
>()("@chemistry/RuntimeStore/Test") {}

/** In-memory atomic runtime store Layer for public-seam tests. */
export const RuntimeStoreTestLayer = Layer.effectContext(
  Effect.gen(function* () {
    const states = yield* Ref.make<ReadonlyMap<string, RuntimeState>>(new Map());
    const semaphore = yield* Semaphore.make(1);
    const nextFailure = yield* Ref.make<
      Option.Option<{
        readonly operation: string;
        readonly position: "before" | "after";
      }>
    >(Option.none());

    const load = Effect.fn("RuntimeStore.Test.load")(function* (
      sessionId: SessionId,
      initial: RuntimeState,
    ) {
      return yield* semaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = yield* Ref.get(states);
          const existing = current.get(sessionId);
          if (existing !== undefined) return existing;
          const next = new Map(current);
          next.set(sessionId, initial);
          yield* Ref.set(states, next);
          return initial;
        }),
      );
    });

    const transact = <A, E>(
      operation: string,
      sessionId: SessionId,
      initial: RuntimeState,
      mutation: (runtime: RuntimeState) => Effect.Effect<RuntimeMutation<A>, E>,
    ): Effect.Effect<A, E | RuntimePersistenceError> =>
      semaphore.withPermits(1)(
        Effect.gen(function* () {
          const configuredFailure = yield* Ref.get(nextFailure);
          const failure =
            Option.isSome(configuredFailure) && configuredFailure.value.operation === operation
              ? configuredFailure
              : Option.none();
          if (Option.isSome(failure)) yield* Ref.set(nextFailure, Option.none());
          if (Option.isSome(failure) && failure.value.position === "before") {
            return yield* new RuntimePersistenceError({
              operation: `${operation}:fault-before-commit`,
              message: "Injected failure before runtime transaction commit",
            });
          }
          const current = yield* Ref.get(states);
          const changed = yield* mutation(current.get(sessionId) ?? initial);
          const next = new Map(current);
          next.set(sessionId, changed.state);
          yield* Ref.set(states, next);
          if (Option.isSome(failure) && failure.value.position === "after") {
            return yield* new RuntimePersistenceError({
              operation: `${operation}:fault-after-commit`,
              message: "Injected failure after runtime transaction commit",
            });
          }
          return changed.value;
        }),
      );

    const service = RuntimeStoreTest.of({
      load,
      transact,
      inspect: Effect.fn("RuntimeStore.Test.inspect")(function* (sessionId) {
        return Option.fromUndefinedOr((yield* Ref.get(states)).get(sessionId));
      }),
      replace: Effect.fn("RuntimeStore.Test.replace")(function* (state) {
        yield* Ref.update(states, (current) => {
          const next = new Map(current);
          next.set(state.sessionId, state);
          return next;
        });
      }),
      failOperation: Effect.fn("RuntimeStore.Test.failOperation")(function* (operation, position) {
        yield* Ref.set(nextFailure, Option.some({ operation, position }));
      }),
    });

    return Context.empty().pipe(
      Context.add(RuntimeStore, service),
      Context.add(RuntimeStoreTest, service),
    );
  }),
);
