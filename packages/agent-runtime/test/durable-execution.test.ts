import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
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

const text = (delta: string) => AgentStreamEvent.cases.TextDelta.make({ delta });

interface RuntimeTestOptions {
  readonly alarmDebounceMs?: number;
  readonly maxRecoveryAttempts?: number;
  readonly maxRecoveryWork?: number;
  readonly maxStreamBytes?: number;
  readonly maxOomStrikes?: number;
  readonly noProgressTimeoutMs?: number;
  readonly stableStateTimeoutMs?: number;
  readonly stallTimeoutMs?: number;
}

const makeTestLayer = (options: RuntimeTestOptions = {}) => {
  const stores = RuntimeStoreTestLayer;
  const turns = TurnExecutorTestLayer;
  const runtime = makeDurableExecutionLayer({
    ...defaultRuntimeLimits,
    alarmDebounceMs: options.alarmDebounceMs ?? 0,
    maxRecoveryAttempts: options.maxRecoveryAttempts ?? defaultRuntimeLimits.maxRecoveryAttempts,
    maxRecoveryWork: options.maxRecoveryWork ?? defaultRuntimeLimits.maxRecoveryWork,
    maxStreamBytes: options.maxStreamBytes ?? defaultRuntimeLimits.maxStreamBytes,
    maxOomStrikes: options.maxOomStrikes ?? defaultRuntimeLimits.maxOomStrikes,
    noProgressTimeoutMs: options.noProgressTimeoutMs ?? defaultRuntimeLimits.noProgressTimeoutMs,
    stableStateTimeoutMs: options.stableStateTimeoutMs ?? defaultRuntimeLimits.stableStateTimeoutMs,
    stallTimeoutMs: options.stallTimeoutMs ?? defaultRuntimeLimits.stallTimeoutMs,
  }).pipe(Layer.provide(stores), Layer.provide(RuntimeIdSourceTestLayer), Layer.provide(turns));
  return Layer.mergeAll(stores, turns, runtime);
};

const run = <A, E>(
  effect: Effect.Effect<A, E, DurableExecution | RuntimeStoreTest | TurnExecutorTest>,
) => Effect.runPromise(effect.pipe(Effect.provide(makeTestLayer())));

describe("Effect-native durable execution", () => {
  test("persists every event before emission and replays strictly after a cursor", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-stream");
        yield* turns.setEvents([text("a"), text("b")]);
        const admission = yield* runtime.admit(
          sessionId,
          "hello",
          SubmissionId.make("submission-stream"),
        );

        expect(admission.operation.kind).toBe("agent-turn");
        expect(admission.operation.input.prompt).toBe("hello");
        expect(admission.operation.requestSnapshot.model).toBe("test-model");
        expect(
          admission.operation.effectLedger.map((entry) => [entry.phase, entry.status]),
        ).toEqual([
          ["request-snapshot", "completed"],
          ["admission", "completed"],
        ]);

        const emitted = yield* runtime.run(sessionId, admission.operation.operationId).pipe(
          Stream.mapEffect((event) =>
            Effect.gen(function* () {
              const stored = yield* store.inspect(sessionId);
              expect(Option.isSome(stored)).toBe(true);
              if (Option.isSome(stored)) {
                const stream = stored.value.streams.find(
                  (candidate) => candidate.streamId === event.streamId,
                );
                const operation = stored.value.operations.find(
                  (candidate) => candidate.operationId === event.operationId,
                );
                expect(
                  stream?.events.some((candidate) => candidate.sequence === event.sequence),
                ).toBe(true);
                expect(operation?.checkpoint).toBe("streaming");
                expect(operation?.leaseExpiresAt).not.toBeNull();
              }
              return event;
            }),
          ),
          Stream.runCollect,
        );

        expect(Array.from(emitted).map((event) => event.sequence)).toEqual([0, 1]);
        const replayAll = yield* runtime.replay(sessionId, admission.operation.streamId, -1);
        expect(replayAll.events.map((event) => event.sequence)).toEqual([0, 1]);
        expect(replayAll.status).toBe("completed");
        const replayTail = yield* runtime.replay(sessionId, admission.operation.streamId, 0);
        expect(replayTail.events.map((event) => event.sequence)).toEqual([1]);
        const staleCursor = yield* runtime
          .replay(sessionId, admission.operation.streamId, 3)
          .pipe(Effect.flip);
        expect(staleCursor._tag).toBe("RuntimeCursorError");
        const terminalState = yield* store.inspect(sessionId);
        if (Option.isSome(terminalState)) {
          const completed = terminalState.value.operations.find(
            (operation) => operation.operationId === admission.operation.operationId,
          );
          expect(
            completed?.effectLedger.some(
              (entry) => entry.phase === "inference" && entry.status === "completed",
            ),
          ).toBe(true);
          expect(
            completed?.effectLedger.some(
              (entry) => entry.phase === "terminal" && entry.status === "completed",
            ),
          ).toBe(true);
        }
      }),
    ));

  test("recovers a first-attempt empty model stream instead of failing the turn", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const sessionId = SessionId.make("runtime-disconnect");
        yield* turns.setFailure(
          Option.some(
            new AgentInferenceError({
              operation: "empty-model-stream",
              message: "Model stream completed without text or tool output",
            }),
          ),
        );
        const admission = yield* runtime.admit(
          sessionId,
          "hello",
          SubmissionId.make("submission-disconnect"),
        );
        yield* runtime
          .run(sessionId, admission.operation.operationId)
          .pipe(Stream.runDrain, Effect.ignore);
        const wake = yield* runtime.wake(sessionId);
        expect(wake.recoverableOperationId).toBe(admission.operation.operationId);
        expect((yield* runtime.terminal(sessionId, admission.operation.streamId))?.status).not.toBe(
          "failed",
        );
        yield* turns.setFailure(Option.none());
        yield* turns.setEvents([text("resumed after disconnect")]);
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        const terminal = yield* runtime.terminal(sessionId, admission.operation.streamId);
        expect(terminal?.status).toBe("completed");
      }),
    ));

  test("converges duplicate submissions and preserves durable FIFO order", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-fifo");
        const submissionId = SubmissionId.make("submission-same");
        yield* turns.setEvents([text("done")]);

        const first = yield* runtime.admit(sessionId, "first", submissionId);
        const duplicate = yield* runtime.admit(sessionId, "ignored duplicate", submissionId);
        const second = yield* runtime.admit(
          sessionId,
          "second",
          SubmissionId.make("submission-second"),
        );

        expect(duplicate.duplicate).toBe(true);
        expect(duplicate.operation.operationId).toBe(first.operation.operationId);
        expect(second.queuePosition).toBe(1);
        const queued = yield* store.inspect(sessionId);
        expect(Option.isSome(queued)).toBe(true);
        if (Option.isSome(queued)) {
          expect(queued.value.queue).toEqual([
            first.operation.operationId,
            second.operation.operationId,
          ]);
        }

        yield* runtime.run(sessionId, first.operation.operationId).pipe(Stream.runDrain);
        yield* runtime.run(sessionId, second.operation.operationId).pipe(Stream.runDrain);
        const executions = yield* turns.executions();
        expect(executions.map((input) => input.prompt)).toEqual(["first", "second"]);
      }),
    ));

  test("restores the oldest durably admitted turn after ephemeral runners disappear", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const sessionId = SessionId.make("runtime-queue-wake");
        yield* turns.setEvents([text("restored")]);
        const first = yield* runtime.admit(
          sessionId,
          "first restored",
          SubmissionId.make("submission-restored-first"),
        );
        const second = yield* runtime.admit(
          sessionId,
          "second restored",
          SubmissionId.make("submission-restored-second"),
        );
        const firstWake = yield* runtime.wake(sessionId);
        expect(firstWake.runnableOperationId).toBe(first.operation.operationId);
        expect(firstWake.runnableStreamId).toBe(first.operation.streamId);
        yield* runtime.run(sessionId, first.operation.operationId).pipe(Stream.runDrain);
        const secondWake = yield* runtime.wake(sessionId);
        expect(secondWake.runnableOperationId).toBe(second.operation.operationId);
        yield* runtime.run(sessionId, second.operation.operationId).pipe(Stream.runDrain);
        expect((yield* turns.executions()).map((input) => input.prompt)).toEqual([
          "first restored",
          "second restored",
        ]);
      }),
    ));

  test("isolates durable queues and streams between named sessions", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        yield* turns.setEvents([text("isolated")]);
        const leftSession = SessionId.make("runtime-left");
        const rightSession = SessionId.make("runtime-right");
        const left = yield* runtime.admit(
          leftSession,
          "left",
          SubmissionId.make("submission-left"),
        );
        const right = yield* runtime.admit(
          rightSession,
          "right",
          SubmissionId.make("submission-right"),
        );
        yield* runtime.run(leftSession, left.operation.operationId).pipe(Stream.runDrain);
        yield* runtime.run(rightSession, right.operation.operationId).pipe(Stream.runDrain);
        const leftState = yield* store.inspect(leftSession);
        const rightState = yield* store.inspect(rightSession);
        if (Option.isSome(leftState) && Option.isSome(rightState)) {
          expect(leftState.value.sessionId).toBe(leftSession);
          expect(rightState.value.sessionId).toBe(rightSession);
          expect(leftState.value.streams[0]?.streamId).not.toBe(
            rightState.value.streams[0]?.streamId,
          );
        }
      }),
    ));

  test("generation-fences a stale producer before its next append", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-fence");
        yield* turns.setEvents([text("first"), text("stale")]);
        const admission = yield* runtime.admit(
          sessionId,
          "fence",
          SubmissionId.make("submission-fence"),
        );
        let observed = 0;
        const failure = yield* runtime.run(sessionId, admission.operation.operationId).pipe(
          Stream.mapEffect((event) =>
            Effect.gen(function* () {
              observed += 1;
              if (observed === 1) {
                const current = yield* store.inspect(sessionId);
                if (Option.isSome(current)) {
                  yield* store.replace(
                    RuntimeState.make({
                      ...current.value,
                      generation: current.value.generation + 1,
                      operations: current.value.operations.map((operation) =>
                        operation.operationId === event.operationId
                          ? { ...operation, generation: operation.generation + 1 }
                          : operation,
                      ),
                    }),
                  );
                }
              }
              return event;
            }),
          ),
          Stream.runCollect,
          Effect.flip,
        );
        expect(failure._tag).toBe("RuntimeFenceError");
        const stored = yield* store.inspect(sessionId);
        if (Option.isSome(stored)) {
          expect(stored.value.streams[0]?.events.map((event) => event.sequence)).toEqual([0]);
        }
      }),
    ));

  test("recovers an expired same-boot lease and fences the abandoned runner", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const store = yield* RuntimeStoreTest;
        const turns = yield* TurnExecutorTest;
        const sessionId = SessionId.make("runtime-expired-same-boot");
        const admission = yield* runtime.admit(
          sessionId,
          "expired",
          SubmissionId.make("submission-expired-same-boot"),
        );
        yield* turns.setEvents([text("stale-runner-output")]);
        yield* turns.pauseExecutions();
        const abandoned = yield* runtime
          .run(sessionId, admission.operation.operationId)
          .pipe(Stream.runDrain, Effect.result, Effect.forkScoped);
        yield* turns.nextExecution();
        const claimed = yield* store.inspect(sessionId);
        if (Option.isNone(claimed)) return;
        yield* store.replace(
          RuntimeState.make({
            ...claimed.value,
            operations: claimed.value.operations.map((operation) =>
              operation.operationId === admission.operation.operationId
                ? { ...operation, leaseExpiresAt: 0 }
                : operation,
            ),
          }),
        );
        const wake = yield* runtime.wake(sessionId);
        expect(wake.recoverableOperationId).toBe(admission.operation.operationId);
        yield* turns.releaseExecution();
        const outcome = yield* Fiber.join(abandoned);
        expect(outcome._tag).toBe("Failure");
        if (outcome._tag === "Failure") {
          expect(outcome.failure._tag).toBe("RuntimeFenceError");
        }
        expect((yield* runtime.replay(sessionId, admission.operation.streamId, -1)).events).toEqual(
          [],
        );
      }).pipe(Effect.scoped),
    ));

  test("rehydrates an interrupted partial and continues under a new generation", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-recovery");
        const admission = yield* runtime.admit(
          sessionId,
          "recover me",
          SubmissionId.make("submission-recovery"),
        );
        const before = yield* store.inspect(sessionId);
        if (Option.isNone(before)) return;
        const operation = before.value.operations.find(
          (candidate) => candidate.operationId === admission.operation.operationId,
        );
        const stream = before.value.streams.find(
          (candidate) => candidate.streamId === admission.operation.streamId,
        );
        if (operation === undefined || stream === undefined) return;

        const partialEvent = DurableStreamEvent.make({
          streamId: stream.streamId,
          operationId: operation.operationId,
          sequence: 0,
          event: text("partial "),
          producedAt: 10,
        });
        const interrupted = RuntimeState.make({
          ...before.value,
          bootId: BootId.make("old-boot"),
          generation: 1,
          operations: before.value.operations.map((candidate) =>
            candidate.operationId === operation.operationId
              ? {
                  ...candidate,
                  status: "running",
                  checkpoint: "streaming",
                  generation: 1,
                  ownerBootId: BootId.make("old-boot"),
                  leaseExpiresAt: 20,
                }
              : candidate,
          ),
          queue: [],
          activeOperationId: operation.operationId,
          streams: before.value.streams.map((candidate) =>
            candidate.streamId === stream.streamId
              ? {
                  ...candidate,
                  status: "open",
                  latestSequence: 0,
                  events: [partialEvent],
                }
              : candidate,
          ),
        });
        yield* store.replace(interrupted);
        yield* turns.setEvents([text("continued")]);

        const wake = yield* runtime.wake(sessionId);
        expect(wake.recoverableOperationId).toBe(operation.operationId);
        const recovered = Array.from(yield* runtime.recover(sessionId).pipe(Stream.runCollect));
        expect(recovered.map((event) => event.sequence)).toEqual([1]);
        const partials = yield* turns.partials();
        expect(partials.map((partial) => partial.text)).toEqual(["partial "]);
        const executions = yield* turns.executions();
        expect(executions.at(-1)?.mode).toBe("continue");
        expect(executions.at(-1)?.operationId).toBe(operation.operationId);
        const completed = yield* store.inspect(sessionId);
        if (Option.isSome(completed)) {
          const recoveredOperation = completed.value.operations.find(
            (candidate) => candidate.operationId === operation.operationId,
          );
          expect(
            recoveredOperation?.effectLedger.some(
              (entry) => entry.phase === "transcript-partial" && entry.status === "completed",
            ),
          ).toBe(true);
        }
      }),
    ));

  test("rejects early and stale recovery alarms before consuming a current due intent", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-alarm-fence");
        yield* runtime.admit(sessionId, "alarm", SubmissionId.make("submission-alarm-fence"));
        const before = yield* store.inspect(sessionId);
        if (Option.isNone(before)) return;
        const operation = before.value.operations[0];
        if (operation === undefined) return;
        yield* store.replace(
          RuntimeState.make({
            ...before.value,
            bootId: BootId.make("alarm-old-boot"),
            operations: [
              {
                ...operation,
                status: "running",
                checkpoint: "preparing",
                generation: 1,
                ownerBootId: BootId.make("alarm-old-boot"),
                leaseExpiresAt: 0,
              },
            ],
            queue: [],
            activeOperationId: operation.operationId,
          }),
        );
        const wake = yield* runtime.wake(sessionId);
        expect(wake.recoveryAlarmAt).not.toBeNull();
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        expect((yield* turns.executions()).length).toBe(0);

        const scheduled = yield* store.inspect(sessionId);
        if (Option.isNone(scheduled) || scheduled.value.alarm?._tag !== "Recover") return;
        yield* store.replace(
          RuntimeState.make({
            ...scheduled.value,
            alarm: {
              ...scheduled.value.alarm,
              generation: scheduled.value.alarm.generation + 1,
              scheduledAt: 0,
            },
            recovery:
              scheduled.value.recovery === null
                ? null
                : { ...scheduled.value.recovery, scheduledAt: 0 },
          }),
        );
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        expect((yield* turns.executions()).length).toBe(0);

        const stale = yield* store.inspect(sessionId);
        if (Option.isNone(stale) || stale.value.recovery === null) return;
        expect(stale.value.alarm).toBeNull();
        const current = stale.value.operations.find(
          (candidate) => candidate.operationId === operation.operationId,
        );
        if (current === undefined) return;
        yield* store.replace(
          RuntimeState.make({
            ...stale.value,
            alarm: {
              _tag: "Recover",
              operationId: operation.operationId,
              generation: current.generation,
              scheduledAt: 0,
            },
            recovery: { ...stale.value.recovery, scheduledAt: 0 },
          }),
        );
        yield* turns.setEvents([text("retried")]);
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        expect((yield* turns.executions()).length).toBe(1);
      }).pipe(Effect.provide(makeTestLayer({ alarmDebounceMs: 60_000 }))),
    ));

  test("converges the transcript-before-runtime-terminal crash window without re-inference", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-terminal-window");
        const admission = yield* runtime.admit(
          sessionId,
          "already persisted",
          SubmissionId.make("submission-terminal-window"),
        );
        const before = yield* store.inspect(sessionId);
        if (Option.isNone(before)) return;
        const operation = before.value.operations[0];
        const stream = before.value.streams[0];
        if (operation === undefined || stream === undefined) return;
        yield* turns.persistPartial({
          sessionId,
          assistantMessageId: operation.assistantMessageId,
          text: "complete transcript answer",
        });
        yield* store.replace(
          RuntimeState.make({
            ...before.value,
            bootId: BootId.make("old-terminal-boot"),
            operations: [
              {
                ...operation,
                status: "running",
                checkpoint: "streaming",
                generation: 1,
                ownerBootId: BootId.make("old-terminal-boot"),
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
                    event: text("complete transcript answer"),
                    producedAt: 1,
                  }),
                ],
              },
            ],
          }),
        );
        yield* runtime.wake(sessionId);
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        expect((yield* turns.executions()).length).toBe(0);
        const terminal = yield* runtime.terminal(sessionId, admission.operation.streamId);
        expect(terminal?.status).toBe("completed");
      }),
    ));

  test("terminalizes instead of executing when the recovery attempt budget is exhausted", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-exhausted");
        const admission = yield* runtime.admit(
          sessionId,
          "never rerun",
          SubmissionId.make("submission-exhausted"),
        );
        const before = yield* store.inspect(sessionId);
        if (Option.isNone(before)) return;
        const operation = before.value.operations[0];
        const stream = before.value.streams[0];
        if (operation === undefined || stream === undefined) return;
        yield* store.replace(
          RuntimeState.make({
            ...before.value,
            bootId: BootId.make("old-boot"),
            operations: [
              {
                ...operation,
                status: "running",
                checkpoint: "streaming",
                generation: 1,
                ownerBootId: BootId.make("old-boot"),
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
                    event: text("partial"),
                    producedAt: 1,
                  }),
                ],
              },
            ],
          }),
        );
        yield* runtime.wake(sessionId);
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        expect((yield* turns.executions()).length).toBe(0);
        const terminal = yield* runtime.terminal(sessionId, admission.operation.streamId);
        expect(terminal?.status).toBe("failed");
        expect(terminal?.reason).toBe("recovery-attempts-exhausted");
        const exhaustedState = yield* store.inspect(sessionId);
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        const repeatedState = yield* store.inspect(sessionId);
        if (Option.isSome(exhaustedState) && Option.isSome(repeatedState)) {
          expect(repeatedState.value.generation).toBe(exhaustedState.value.generation);
          expect(repeatedState.value.streams[0]?.terminalReason).toBe(
            "recovery-attempts-exhausted",
          );
        }
      }).pipe(Effect.provide(makeTestLayer({ maxRecoveryAttempts: 0 }))),
    ));

  test("parks explicit interaction checkpoints without re-executing", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-parked");
        yield* runtime.admit(sessionId, "park", SubmissionId.make("submission-parked"));
        const before = yield* store.inspect(sessionId);
        if (Option.isNone(before)) return;
        const operation = before.value.operations[0];
        if (operation === undefined) return;
        yield* store.replace(
          RuntimeState.make({
            ...before.value,
            bootId: BootId.make("old-parked-boot"),
            operations: [
              {
                ...operation,
                status: "parked",
                checkpoint: "parked",
                generation: 1,
                ownerBootId: BootId.make("old-parked-boot"),
              },
            ],
            queue: [],
            activeOperationId: operation.operationId,
          }),
        );
        const wake = yield* runtime.wake(sessionId);
        expect(wake.recoveryAlarmAt).toBeNull();
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        expect((yield* turns.executions()).length).toBe(0);
        expect((yield* runtime.probe(sessionId)).snapshot.activeOperation?.status).toBe("parked");
      }),
    ));

  test("bounds waiting for serialized turn ownership to stabilize", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const sessionId = SessionId.make("runtime-stable-timeout");
        yield* turns.setNever(true);
        const first = yield* runtime.admit(
          sessionId,
          "first never ends",
          SubmissionId.make("submission-stable-first"),
        );
        const second = yield* runtime.admit(
          sessionId,
          "second is bounded",
          SubmissionId.make("submission-stable-second"),
        );
        const active = yield* runtime
          .run(sessionId, first.operation.operationId)
          .pipe(Stream.runDrain, Effect.forkScoped);
        yield* turns.nextExecution();
        const failure = yield* runtime
          .run(sessionId, second.operation.operationId)
          .pipe(Stream.runDrain, Effect.flip);
        expect(failure._tag).toBe("RuntimeTransitionError");
        if (failure._tag === "RuntimeTransitionError") {
          expect(failure.operation).toBe("stable-state");
        }
        yield* Fiber.interrupt(active);
      }).pipe(
        Effect.scoped,
        Effect.provide(makeTestLayer({ stableStateTimeoutMs: 0, stallTimeoutMs: 60_000 })),
      ),
    ));

  test("routes a live stall into durable bounded recovery", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const sessionId = SessionId.make("runtime-stall");
        yield* turns.setNever(true);
        const admission = yield* runtime.admit(
          sessionId,
          "stall",
          SubmissionId.make("submission-stall"),
        );
        yield* runtime.run(sessionId, admission.operation.operationId).pipe(Stream.runDrain);
        const interrupted = yield* runtime.terminal(sessionId, admission.operation.streamId);
        expect(interrupted?.status).toBe("interrupted");
        expect(interrupted?.reason).toContain("produced no event");
        const wake = yield* runtime.wake(sessionId);
        expect(wake.recoverableOperationId).toBe(admission.operation.operationId);
      }).pipe(Effect.provide(makeTestLayer({ stallTimeoutMs: 0 }))),
    ));

  test("bounds repeated memory-reset recovery failures with a durable strike counter", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-oom");
        const admission = yield* runtime.admit(
          sessionId,
          "oom",
          SubmissionId.make("submission-oom"),
        );
        const before = yield* store.inspect(sessionId);
        if (Option.isNone(before)) return;
        const operation = before.value.operations[0];
        const stream = before.value.streams[0];
        if (operation === undefined || stream === undefined) return;
        yield* store.replace(
          RuntimeState.make({
            ...before.value,
            bootId: BootId.make("old-oom-boot"),
            operations: [
              {
                ...operation,
                status: "running",
                checkpoint: "streaming",
                generation: 1,
                ownerBootId: BootId.make("old-oom-boot"),
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
                    event: text("partial"),
                    producedAt: 1,
                  }),
                ],
              },
            ],
          }),
        );
        yield* turns.setFailure(
          Option.some(
            new AgentInferenceError({
              operation: "stream-text",
              message: "Durable Object's isolate exceeded its memory limit and was reset",
            }),
          ),
        );
        yield* runtime.wake(sessionId);
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        yield* runtime.wake(sessionId);
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        const terminal = yield* runtime.terminal(sessionId, admission.operation.streamId);
        expect(terminal?.status).toBe("failed");
        expect(terminal?.reason).toBe("recovery-oom-exhausted");
        expect((yield* turns.executions()).length).toBe(1);
      }).pipe(Effect.provide(makeTestLayer({ maxOomStrikes: 1 }))),
    ));

  test("enforces recovery work and no-progress budgets independently", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-work-budget");
        const admission = yield* runtime.admit(
          sessionId,
          "work",
          SubmissionId.make("submission-work"),
        );
        const before = yield* store.inspect(sessionId);
        if (Option.isNone(before)) return;
        const operation = before.value.operations[0];
        const stream = before.value.streams[0];
        if (operation === undefined || stream === undefined) return;
        yield* store.replace(
          RuntimeState.make({
            ...before.value,
            bootId: BootId.make("old-work-boot"),
            operations: [
              {
                ...operation,
                status: "running",
                checkpoint: "streaming",
                generation: 1,
                ownerBootId: BootId.make("old-work-boot"),
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
                    event: text("partial"),
                    producedAt: 1,
                  }),
                ],
              },
            ],
          }),
        );
        yield* turns.setEvents([text("one"), text("over-budget")]);
        yield* runtime.wake(sessionId);
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        yield* runtime.wake(sessionId);
        yield* runtime.recover(sessionId).pipe(Stream.runDrain);
        const terminal = yield* runtime.terminal(sessionId, admission.operation.streamId);
        expect(terminal?.reason).toBe("recovery-work-exhausted");

        const noProgressSession = SessionId.make("runtime-no-progress");
        const noProgressAdmission = yield* runtime.admit(
          noProgressSession,
          "idle",
          SubmissionId.make("submission-no-progress"),
        );
        const idle = yield* store.inspect(noProgressSession);
        if (Option.isNone(idle)) return;
        const idleOperation = idle.value.operations[0];
        const idleStream = idle.value.streams[0];
        if (idleOperation === undefined || idleStream === undefined) return;
        yield* store.replace(
          RuntimeState.make({
            ...idle.value,
            bootId: BootId.make("old-idle-boot"),
            operations: [
              {
                ...idleOperation,
                status: "running",
                checkpoint: "streaming",
                generation: 1,
                ownerBootId: BootId.make("old-idle-boot"),
              },
            ],
            queue: [],
            activeOperationId: idleOperation.operationId,
          }),
        );
        yield* runtime.wake(noProgressSession);
        const scheduled = yield* store.inspect(noProgressSession);
        if (Option.isSome(scheduled) && scheduled.value.recovery !== null) {
          yield* store.replace(
            RuntimeState.make({
              ...scheduled.value,
              recovery: { ...scheduled.value.recovery, lastProgressAt: 0 },
            }),
          );
        }
        yield* runtime.recover(noProgressSession).pipe(Stream.runDrain);
        const idleTerminal = yield* runtime.terminal(
          noProgressSession,
          noProgressAdmission.operation.streamId,
        );
        expect(idleTerminal?.reason).toBe("recovery-no-progress-timeout");
      }).pipe(Effect.provide(makeTestLayer({ maxRecoveryWork: 1 }))),
    ));

  test("rejects an oversized durable event before persistence or publication", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const sessionId = SessionId.make("runtime-event-size");
        yield* turns.setEvents([text("x".repeat(defaultRuntimeLimits.maxEventBytes + 1))]);
        const admission = yield* runtime.admit(
          sessionId,
          "oversized event",
          SubmissionId.make("submission-event-size"),
        );
        const failure = yield* runtime
          .run(sessionId, admission.operation.operationId)
          .pipe(Stream.runDrain, Effect.flip);
        expect(failure._tag).toBe("RuntimeCapacityError");
        if (failure._tag === "RuntimeCapacityError") {
          expect(failure.resource).toBe("stream-event-bytes");
        }
        expect((yield* runtime.replay(sessionId, admission.operation.streamId, -1)).events).toEqual(
          [],
        );
      }),
    ));

  test("accepts a token-chunked 500-word response under the default stream budget", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-500-word-response");
        const responseEvents = Array.from({ length: 500 }, () => [text("lorem"), text(" ")]).flat();
        yield* turns.setEvents(responseEvents);
        const admission = yield* runtime.admit(
          sessionId,
          "write me a 500 word lorem ipsum",
          SubmissionId.make("submission-500-word-response"),
        );

        yield* runtime.run(sessionId, admission.operation.operationId).pipe(Stream.runDrain);

        const replay = yield* runtime.replay(sessionId, admission.operation.streamId, -1);
        const persisted = yield* store.inspect(sessionId);
        const persistedStream = Option.isSome(persisted)
          ? persisted.value.streams.find(
              (candidate) => candidate.streamId === admission.operation.streamId,
            )
          : undefined;
        expect(replay.status).toBe("completed");
        expect(replay.events).toHaveLength(responseEvents.length);
        expect(persistedStream?.encodedBytes).toBeGreaterThan(128 * 1_024);
      }),
    ));

  test("rejects a stream that crosses its cumulative byte budget", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const sessionId = SessionId.make("runtime-stream-size");
        yield* turns.setEvents([text("small")]);
        const admission = yield* runtime.admit(
          sessionId,
          "oversized stream",
          SubmissionId.make("submission-stream-size"),
        );
        const failure = yield* runtime
          .run(sessionId, admission.operation.operationId)
          .pipe(Stream.runDrain, Effect.flip);
        expect(failure._tag).toBe("RuntimeCapacityError");
        if (failure._tag === "RuntimeCapacityError") {
          expect(failure.resource).toBe("stream-bytes");
        }
        expect((yield* runtime.replay(sessionId, admission.operation.streamId, -1)).events).toEqual(
          [],
        );
      }).pipe(Effect.provide(makeTestLayer({ maxStreamBytes: 1 }))),
    ));

  test("removes expired streams and enforces bounded terminal retention", () =>
    run(
      Effect.gen(function* () {
        const runtime = yield* DurableExecution;
        const turns = yield* TurnExecutorTest;
        const store = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("runtime-cleanup");
        yield* turns.setEvents([text("done")]);
        for (let index = 0; index < defaultRuntimeLimits.maxRetainedStreams + 2; index += 1) {
          const admission = yield* runtime.admit(
            sessionId,
            `clean-${index}`,
            SubmissionId.make(`submission-clean-${index}`),
          );
          yield* runtime.run(sessionId, admission.operation.operationId).pipe(Stream.runDrain);
        }
        expect(yield* runtime.cleanup(sessionId)).toBe(2);

        const afterBound = yield* store.inspect(sessionId);
        if (Option.isNone(afterBound)) return;
        const expiring = afterBound.value.streams[0];
        if (expiring === undefined) return;
        yield* store.replace(
          RuntimeState.make({
            ...afterBound.value,
            streams: afterBound.value.streams.map((stream) =>
              stream.streamId === expiring.streamId ? { ...stream, expiresAt: 0 } : stream,
            ),
          }),
        );
        expect(yield* runtime.cleanup(sessionId)).toBe(1);
        const replayFailure = yield* runtime
          .replay(sessionId, expiring.streamId, -1)
          .pipe(Effect.flip);
        expect(replayFailure._tag).toBe("RuntimeTransitionError");
      }),
    ));
});
