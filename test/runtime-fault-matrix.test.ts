import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import {
  AgentStreamEvent,
  BootId,
  DurableStreamEvent,
  SessionId,
  SubmissionId,
} from "../src/shared/agent-protocol.ts";
import { DurableExecution, DurableExecutionLive } from "../src/server/durable-execution.ts";
import { RuntimeState } from "../src/server/runtime-state.ts";
import {
  RuntimeIdSourceTestLayer,
  TurnExecutorTest,
  TurnExecutorTestLayer,
} from "./support/durable-runtime-test-layers.ts";
import { RuntimeStoreTest, RuntimeStoreTestLayer } from "./support/runtime-store-test-layer.ts";

const stores = RuntimeStoreTestLayer;
const turns = TurnExecutorTestLayer;
const runtimeLayer = DurableExecutionLive.pipe(
  Layer.provide(stores),
  Layer.provide(RuntimeIdSourceTestLayer),
  Layer.provide(turns),
);
const testLayer = Layer.mergeAll(stores, turns, runtimeLayer);

const run = <A, E>(
  effect: Effect.Effect<A, E, DurableExecution | RuntimeStoreTest | TurnExecutorTest | Scope.Scope>,
) => Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(testLayer)));

const delta = (value: string) => AgentStreamEvent.cases.TextDelta.make({ delta: value });

describe("durable transition crash matrix", () => {
  test("admission converges across failures before and after commit", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("fault-admit");
        const submissionId = SubmissionId.make("fault-admit-key");

        yield* store.failNextTransaction("before");
        const beforeFailure = yield* runtime
          .admit(sessionId, "hello", submissionId)
          .pipe(Effect.flip);
        expect(beforeFailure._tag).toBe("RuntimePersistenceError");
        expect(Option.isNone(yield* store.inspect(sessionId))).toBe(true);

        yield* store.failNextTransaction("after");
        const afterFailure = yield* runtime
          .admit(sessionId, "hello", submissionId)
          .pipe(Effect.flip);
        expect(afterFailure._tag).toBe("RuntimePersistenceError");
        const retried = yield* runtime.admit(sessionId, "hello", submissionId);
        expect(retried.duplicate).toBe(true);
        const persisted = yield* store.inspect(sessionId);
        if (Option.isSome(persisted)) {
          expect(persisted.value.operations.length).toBe(1);
          expect(persisted.value.queue.length).toBe(1);
        }
      }),
    ));

  test("claim is retryable whether the failure happens before or after commit", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const executor = yield* TurnExecutorTest;
        const sessionId = SessionId.make("fault-claim-before");
        yield* executor.setEvents([delta("ok")]);
        const before = yield* runtime.admit(
          sessionId,
          "before",
          SubmissionId.make("fault-claim-before-key"),
        );
        yield* store.failNextTransaction("before");
        expect(
          (yield* runtime
            .run(sessionId, before.operation.operationId)
            .pipe(Stream.runDrain, Effect.flip))._tag,
        ).toBe("RuntimePersistenceError");
        yield* runtime.run(sessionId, before.operation.operationId).pipe(Stream.runDrain);

        const afterSession = SessionId.make("fault-claim-after");
        const after = yield* runtime.admit(
          afterSession,
          "after",
          SubmissionId.make("fault-claim-after-key"),
        );
        yield* store.failNextTransaction("after");
        expect(
          (yield* runtime
            .run(afterSession, after.operation.operationId)
            .pipe(Stream.runDrain, Effect.flip))._tag,
        ).toBe("RuntimePersistenceError");
        yield* runtime.run(afterSession, after.operation.operationId).pipe(Stream.runDrain);
        expect((yield* runtime.terminal(afterSession, after.operation.streamId))?.status).toBe(
          "completed",
        );
      }),
    ));

  test("append never publishes an uncommitted event and replays a committed-but-unacknowledged event", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const executor = yield* TurnExecutorTest;
        yield* executor.setEvents([delta("event")]);
        yield* executor.pauseExecutions();

        const beforeSession = SessionId.make("fault-append-before");
        const before = yield* runtime.admit(
          beforeSession,
          "before append",
          SubmissionId.make("fault-append-before-key"),
        );
        const beforeFiber = yield* runtime
          .run(beforeSession, before.operation.operationId)
          .pipe(Stream.runDrain, Effect.result, Effect.forkScoped);
        yield* executor.nextExecution();
        yield* store.failNextTransaction("before");
        yield* executor.releaseExecution();
        yield* Fiber.join(beforeFiber);
        expect(
          (yield* runtime.replay(beforeSession, before.operation.streamId, -1)).events,
        ).toEqual([]);

        const afterSession = SessionId.make("fault-append-after");
        const after = yield* runtime.admit(
          afterSession,
          "after append",
          SubmissionId.make("fault-append-after-key"),
        );
        const afterFiber = yield* runtime
          .run(afterSession, after.operation.operationId)
          .pipe(Stream.runDrain, Effect.result, Effect.forkScoped);
        yield* executor.nextExecution();
        yield* store.failNextTransaction("after");
        yield* executor.releaseExecution();
        yield* Fiber.join(afterFiber);
        const replay = yield* runtime.replay(afterSession, after.operation.streamId, -1);
        expect(replay.events.map((event) => event.sequence)).toEqual([0]);
      }),
    ));

  test("a terminal committed before acknowledgement remains the single terminal outcome", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const executor = yield* TurnExecutorTest;
        const sessionId = SessionId.make("fault-terminal-after");
        yield* executor.setEvents([delta("done")]);
        const admission = yield* runtime.admit(
          sessionId,
          "terminal",
          SubmissionId.make("fault-terminal-key"),
        );
        const outcome = yield* runtime.run(sessionId, admission.operation.operationId).pipe(
          Stream.mapEffect((event) => store.failNextTransaction("after").pipe(Effect.as(event))),
          Stream.runDrain,
          Effect.result,
        );
        expect(outcome._tag).toBe("Failure");
        expect((yield* runtime.terminal(sessionId, admission.operation.streamId))?.status).toBe(
          "completed",
        );
        const replayed = yield* runtime
          .run(sessionId, admission.operation.operationId)
          .pipe(Stream.runCollect);
        expect(Array.from(replayed).map((event) => event.sequence)).toEqual([0]);
        const state = yield* store.inspect(sessionId);
        if (Option.isSome(state)) {
          expect(state.value.streams.length).toBe(1);
          expect(state.value.operations.length).toBe(1);
        }
      }),
    ));

  test("wake incident creation is retryable and preserves one incident after post-commit failure", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("fault-wake");
        const admission = yield* runtime.admit(
          sessionId,
          "wake",
          SubmissionId.make("fault-wake-key"),
        );
        const before = yield* store.inspect(sessionId);
        if (Option.isNone(before)) return;
        const operation = before.value.operations[0];
        const stream = before.value.streams[0];
        if (operation === undefined || stream === undefined) return;
        yield* store.replace(
          RuntimeState.make({
            ...before.value,
            bootId: BootId.make("fault-old-boot"),
            operations: [
              {
                ...operation,
                status: "running",
                checkpoint: "streaming",
                generation: 1,
                ownerBootId: BootId.make("fault-old-boot"),
              },
            ],
            queue: [],
            activeOperationId: operation.operationId,
            streams: [
              {
                ...stream,
                latestSequence: 0,
                events: [
                  DurableStreamEvent.make({
                    streamId: stream.streamId,
                    operationId: operation.operationId,
                    sequence: 0,
                    event: delta("partial"),
                    producedAt: 1,
                  }),
                ],
              },
            ],
          }),
        );
        yield* store.failNextTransaction("after");
        expect((yield* runtime.wake(sessionId).pipe(Effect.flip))._tag).toBe(
          "RuntimePersistenceError",
        );
        const retry = yield* runtime.wake(sessionId);
        expect(retry.recoverableOperationId).toBe(admission.operation.operationId);
        const state = yield* store.inspect(sessionId);
        if (Option.isSome(state)) {
          expect(state.value.recovery?.operationId).toBe(admission.operation.operationId);
          expect(state.value.recovery?.attempt).toBe(0);
        }
      }),
    ));
});
