import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import {
  AgentInferenceError,
  AgentStreamEvent,
  BootId,
  DurableStreamEvent,
  SessionId,
  SubmissionId,
} from "@chemistry/contracts/agent-protocol";
import {
  DurableExecution,
  makeDurableExecutionLayer,
} from "@chemistry/agent-runtime/durable-execution";
import { RuntimeState, defaultRuntimeLimits } from "@chemistry/agent-runtime/runtime-state";
import {
  RuntimeIdSourceTestLayer,
  TurnExecutorTest,
  TurnExecutorTestLayer,
} from "./support/durable-runtime-test-layers.ts";
import { RuntimeStoreTest, RuntimeStoreTestLayer } from "./support/runtime-store-test-layer.ts";

const stores = RuntimeStoreTestLayer;
const turns = TurnExecutorTestLayer;
const runtimeLayer = makeDurableExecutionLayer({
  ...defaultRuntimeLimits,
  alarmDebounceMs: 0,
}).pipe(Layer.provide(stores), Layer.provide(RuntimeIdSourceTestLayer), Layer.provide(turns));
const testLayer = Layer.mergeAll(stores, turns, runtimeLayer);

const run = <A, E>(
  effect: Effect.Effect<A, E, DurableExecution | RuntimeStoreTest | TurnExecutorTest | Scope.Scope>,
) => Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(testLayer)));

const delta = (value: string) => AgentStreamEvent.cases.TextDelta.make({ delta: value });

const installInterruptedFixture = Effect.fn("FaultMatrix.installInterruptedFixture")(function* (
  sessionId: SessionId,
  withPartial: boolean,
) {
  const runtime = yield* DurableExecution;
  const store = yield* RuntimeStoreTest;
  const admission = yield* runtime.admit(
    sessionId,
    "recover fixture",
    SubmissionId.make(`submission-${sessionId}`),
  );
  const stored = yield* store.inspect(sessionId);
  if (Option.isNone(stored)) return admission;
  const operation = stored.value.operations[0];
  const stream = stored.value.streams[0];
  if (operation === undefined || stream === undefined) return admission;
  const oldBootId = BootId.make(`old-${sessionId}`);
  const events = withPartial
    ? [
        DurableStreamEvent.make({
          streamId: stream.streamId,
          operationId: operation.operationId,
          sequence: 0,
          event: delta("partial "),
          producedAt: 1,
        }),
      ]
    : [];
  yield* store.replace(
    RuntimeState.make({
      ...stored.value,
      bootId: oldBootId,
      operations: [
        {
          ...operation,
          status: "running",
          checkpoint: withPartial ? "streaming" : "preparing",
          generation: 1,
          ownerBootId: oldBootId,
          leaseExpiresAt: 0,
        },
      ],
      queue: [],
      activeOperationId: operation.operationId,
      streams: [
        {
          ...stream,
          latestSequence: withPartial ? 0 : -1,
          encodedBytes: withPartial ? 128 : 0,
          events,
        },
      ],
    }),
  );
  yield* runtime.wake(sessionId);
  return admission;
});

const expireActiveLease = Effect.fn("FaultMatrix.expireActiveLease")(function* (
  sessionId: SessionId,
) {
  const store = yield* RuntimeStoreTest;
  const stored = yield* store.inspect(sessionId);
  if (Option.isNone(stored) || stored.value.activeOperationId === null) return;
  yield* store.replace(
    RuntimeState.make({
      ...stored.value,
      operations: stored.value.operations.map((operation) =>
        operation.operationId === stored.value.activeOperationId
          ? { ...operation, leaseExpiresAt: 0 }
          : operation,
      ),
    }),
  );
});

describe("durable transition crash matrix", () => {
  test("admission converges across failures before and after commit", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("fault-admit");
        const submissionId = SubmissionId.make("fault-admit-key");

        yield* store.failOperation("admit", "before");
        const beforeFailure = yield* runtime
          .admit(sessionId, "hello", submissionId)
          .pipe(Effect.flip);
        expect(beforeFailure._tag).toBe("RuntimePersistenceError");
        expect(Option.isNone(yield* store.inspect(sessionId))).toBe(true);

        yield* store.failOperation("admit", "after");
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
        yield* store.failOperation("claim", "before");
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
        yield* store.failOperation("claim", "after");
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

  test("request-snapshot persistence converges across failures before and after commit", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const executor = yield* TurnExecutorTest;
        yield* executor.setEvents([delta("snapshotted")]);
        for (const position of ["before", "after"] as const) {
          const sessionId = SessionId.make(`fault-snapshot-${position}`);
          const admission = yield* runtime.admit(
            sessionId,
            "snapshot",
            SubmissionId.make(`submission-snapshot-${position}`),
          );
          yield* store.failOperation("persist-request-snapshot", position);
          expect(
            (yield* runtime
              .run(sessionId, admission.operation.operationId)
              .pipe(Stream.runDrain, Effect.flip))._tag,
          ).toBe("RuntimePersistenceError");
          yield* runtime.run(sessionId, admission.operation.operationId).pipe(Stream.runDrain);
          const state = yield* store.inspect(sessionId);
          if (Option.isSome(state)) {
            const operation = state.value.operations.find(
              (candidate) => candidate.operationId === admission.operation.operationId,
            );
            expect(operation?.requestSnapshot.model).toBe("test-model");
            expect(
              operation?.effectLedger.some(
                (entry) =>
                  entry.phase === "request-snapshot" &&
                  entry.generation > 0 &&
                  entry.status === "completed",
              ),
            ).toBe(true);
          }
        }
      }),
    ));

  test("mark-streaming converges across failures before and after commit", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const executor = yield* TurnExecutorTest;
        yield* executor.setEvents([delta("marked")]);
        for (const position of ["before", "after"] as const) {
          const sessionId = SessionId.make(`fault-mark-${position}`);
          const admission = yield* runtime.admit(
            sessionId,
            "mark",
            SubmissionId.make(`submission-mark-${position}`),
          );
          yield* store.failOperation("mark-streaming", position);
          expect(
            (yield* runtime
              .run(sessionId, admission.operation.operationId)
              .pipe(Stream.runDrain, Effect.flip))._tag,
          ).toBe("RuntimePersistenceError");
          yield* runtime.run(sessionId, admission.operation.operationId).pipe(Stream.runDrain);
          expect((yield* runtime.terminal(sessionId, admission.operation.streamId))?.status).toBe(
            "completed",
          );
        }
      }),
    ));

  test("recovery scheduling converges across failures before and after commit", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const executor = yield* TurnExecutorTest;
        for (const position of ["before", "after"] as const) {
          const sessionId = SessionId.make(`fault-schedule-${position}`);
          const admission = yield* runtime.admit(
            sessionId,
            "schedule",
            SubmissionId.make(`submission-schedule-${position}`),
          );
          yield* executor.setFailure(
            Option.some(
              new AgentInferenceError({
                operation: "fault-memory-reset",
                message: "Worker exceeded its memory limit",
              }),
            ),
          );
          yield* store.failOperation("schedule-recovery", position);
          expect(
            (yield* runtime
              .run(sessionId, admission.operation.operationId)
              .pipe(Stream.runDrain, Effect.flip))._tag,
          ).toBe("RuntimePersistenceError");
          yield* executor.setFailure(Option.none());
          yield* executor.setEvents([delta("scheduled")]);
          if (position === "before") {
            yield* runtime.run(sessionId, admission.operation.operationId).pipe(Stream.runDrain);
          } else {
            yield* runtime.recover(sessionId).pipe(Stream.runDrain);
          }
          expect((yield* runtime.terminal(sessionId, admission.operation.streamId))?.status).toBe(
            "completed",
          );
        }
      }),
    ));

  test("stale alarm-intent consumption converges before and after commit", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        for (const position of ["before", "after"] as const) {
          const sessionId = SessionId.make(`fault-stale-alarm-${position}`);
          yield* installInterruptedFixture(sessionId, false);
          yield* runtime.wake(sessionId);
          const scheduled = yield* store.inspect(sessionId);
          if (
            Option.isNone(scheduled) ||
            scheduled.value.alarm?._tag !== "Recover" ||
            scheduled.value.recovery === null
          ) {
            return;
          }
          yield* store.replace(
            RuntimeState.make({
              ...scheduled.value,
              alarm: {
                ...scheduled.value.alarm,
                generation: scheduled.value.alarm.generation + 1,
              },
            }),
          );
          yield* store.failOperation("prepare-recovery", position);
          expect((yield* runtime.recover(sessionId).pipe(Stream.runDrain, Effect.flip))._tag).toBe(
            "RuntimePersistenceError",
          );
          yield* runtime.recover(sessionId).pipe(Stream.runDrain);
          const consumed = yield* store.inspect(sessionId);
          if (Option.isNone(consumed)) return;
          expect(consumed.value.alarm).toBeNull();
          expect(consumed.value.recovery?.scheduledAt).toBeNull();
          const wake = yield* runtime.wake(sessionId);
          expect(wake.recoveryAlarmAt).not.toBeNull();
          const rearmed = yield* store.inspect(sessionId);
          if (Option.isSome(rearmed) && rearmed.value.alarm?._tag === "Recover") {
            const operation = rearmed.value.operations[0];
            if (operation === undefined) return;
            expect(rearmed.value.alarm.generation).toBe(operation.generation);
          }
        }
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
        yield* store.failOperation("append", "before");
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
        yield* store.failOperation("append", "after");
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
          Stream.mapEffect((event) =>
            store.failOperation("settle", "after").pipe(Effect.as(event)),
          ),
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
        yield* store.failOperation("wake", "after");
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

  test("recovery preparation converges across failures before and after claim commit", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const executor = yield* TurnExecutorTest;
        for (const position of ["before", "after"] as const) {
          const sessionId = SessionId.make(`fault-prepare-${position}`);
          const admission = yield* installInterruptedFixture(sessionId, false);
          yield* executor.setEvents([delta("prepared")]);
          yield* store.failOperation("prepare-recovery", position);
          expect((yield* runtime.recover(sessionId).pipe(Stream.runDrain, Effect.flip))._tag).toBe(
            "RuntimePersistenceError",
          );
          if (position === "after") {
            yield* expireActiveLease(sessionId);
            yield* runtime.wake(sessionId);
          }
          yield* runtime.recover(sessionId).pipe(Stream.runDrain);
          expect((yield* runtime.terminal(sessionId, admission.operation.streamId))?.status).toBe(
            "completed",
          );
        }
      }),
    ));

  test("partial-effect ledger converges every begin/complete commit window", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const executor = yield* TurnExecutorTest;
        for (const operation of ["begin-phase-effect", "complete-phase-effect"] as const) {
          for (const position of ["before", "after"] as const) {
            const sessionId = SessionId.make(`fault-partial-${operation}-${position}`);
            const admission = yield* installInterruptedFixture(sessionId, true);
            yield* executor.setEvents([delta("continued")]);
            yield* store.failOperation(operation, position);
            expect(
              (yield* runtime.recover(sessionId).pipe(Stream.runDrain, Effect.flip))._tag,
            ).toBe("RuntimePersistenceError");
            yield* expireActiveLease(sessionId);
            yield* runtime.wake(sessionId);
            yield* runtime.recover(sessionId).pipe(Stream.runDrain);
            const partials = yield* executor.partials();
            const matching = partials.filter((partial) => partial.sessionId === sessionId);
            expect(matching.length).toBeGreaterThanOrEqual(1);
            expect(matching.length).toBeLessThanOrEqual(2);
            if (operation === "complete-phase-effect" && position === "after") {
              expect(matching.length).toBe(1);
            }
            expect(new Set(matching.map((partial) => partial.assistantMessageId)).size).toBe(1);
            expect((yield* runtime.terminal(sessionId, admission.operation.streamId))?.status).toBe(
              "completed",
            );
          }
        }
      }),
    ));

  test("transcript reconciliation is idempotent before and after terminal commit", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const executor = yield* TurnExecutorTest;
        for (const position of ["before", "after"] as const) {
          const sessionId = SessionId.make(`fault-reconcile-${position}`);
          const admission = yield* installInterruptedFixture(sessionId, true);
          yield* executor.persistPartial({
            sessionId,
            assistantMessageId: admission.operation.assistantMessageId,
            text: "partial ",
          });
          yield* store.failOperation("reconcile-terminal", position);
          expect((yield* runtime.recover(sessionId).pipe(Stream.runDrain, Effect.flip))._tag).toBe(
            "RuntimePersistenceError",
          );
          yield* runtime.recover(sessionId).pipe(Stream.runDrain);
          expect((yield* runtime.terminal(sessionId, admission.operation.streamId))?.status).toBe(
            "completed",
          );
          expect(
            (yield* executor.executions()).filter((input) => input.sessionId === sessionId),
          ).toEqual([]);
        }
      }),
    ));

  test("cleanup converges across failures before and after deletion commit", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const executor = yield* TurnExecutorTest;
        yield* executor.setEvents([delta("cleanup")]);
        for (const position of ["before", "after"] as const) {
          const sessionId = SessionId.make(`fault-cleanup-${position}`);
          const admission = yield* runtime.admit(
            sessionId,
            "cleanup",
            SubmissionId.make(`submission-cleanup-${position}`),
          );
          yield* runtime.run(sessionId, admission.operation.operationId).pipe(Stream.runDrain);
          const stored = yield* store.inspect(sessionId);
          if (Option.isNone(stored)) continue;
          yield* store.replace(
            RuntimeState.make({
              ...stored.value,
              streams: stored.value.streams.map((stream) => ({
                ...stream,
                expiresAt: 0,
              })),
            }),
          );
          yield* store.failOperation("cleanup", position);
          expect((yield* runtime.cleanup(sessionId).pipe(Effect.flip))._tag).toBe(
            "RuntimePersistenceError",
          );
          const removed = yield* runtime.cleanup(sessionId);
          expect(removed).toBe(position === "before" ? 1 : 0);
          const finalState = yield* store.inspect(sessionId);
          if (Option.isSome(finalState)) expect(finalState.value.streams).toEqual([]);
        }
      }),
    ));
});
