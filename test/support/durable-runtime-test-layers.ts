import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import {
  BootId,
  ConnectionId,
  OperationId,
  StreamId,
  SubmissionId,
  type AgentRpcError,
  type AgentStreamEvent,
  type MessageId,
  type SessionId,
} from "../../src/shared/agent-protocol.ts";
import { RuntimeIdSource } from "../../src/server/runtime-id-source.ts";
import {
  TurnExecutor,
  type PersistPartialInput,
  type TurnExecutionInput,
  type TurnExecutorOperations,
} from "../../src/server/turn-executor.ts";

/** Deterministic controls and observations for the turn-executor test adapter. */
export interface TurnExecutorTestOperations extends TurnExecutorOperations {
  /** Replace the finite event script emitted by future executions. */
  readonly setEvents: (events: ReadonlyArray<AgentStreamEvent>) => Effect.Effect<void>;
  /** Fail the next and subsequent executions, or clear the failure. */
  readonly setFailure: (error: Option.Option<AgentRpcError>) => Effect.Effect<void>;
  /** Pause future executions after their start signal and before their first event. */
  readonly pauseExecutions: () => Effect.Effect<void>;
  /** Release one execution paused before its first event. */
  readonly releaseExecution: () => Effect.Effect<void>;
  /** Switch future executions between the finite script and a never-emitting stream. */
  readonly setNever: (enabled: boolean) => Effect.Effect<void>;
  /** Await the next execution start without polling or sleeping. */
  readonly nextExecution: () => Effect.Effect<TurnExecutionInput>;
  /** Read every durable execution input observed by the adapter. */
  readonly executions: () => Effect.Effect<ReadonlyArray<TurnExecutionInput>>;
  /** Read every reconstructed partial persisted by recovery. */
  readonly partials: () => Effect.Effect<ReadonlyArray<PersistPartialInput>>;
}

/** Test-only service backed by the same object as {@link TurnExecutor}. */
export class TurnExecutorTest extends Context.Service<
  TurnExecutorTest,
  TurnExecutorTestOperations
>()("@chemistry/TurnExecutor/Test") {}

/** Deterministic runtime identity Layer with monotonic readable identifiers. */
export const RuntimeIdSourceTestLayer = Layer.effect(
  RuntimeIdSource,
  Effect.gen(function* () {
    const counter = yield* Ref.make(0);
    const next = Effect.fn("RuntimeIdSource.Test.next")(function* (prefix: string) {
      return `${prefix}-${yield* Ref.getAndUpdate(counter, (value) => value + 1)}`;
    });
    return RuntimeIdSource.of({
      submission: () => next("submission").pipe(Effect.map(SubmissionId.make)),
      operation: () => next("operation").pipe(Effect.map(OperationId.make)),
      stream: () => next("stream").pipe(Effect.map(StreamId.make)),
      connection: () => next("connection").pipe(Effect.map(ConnectionId.make)),
      boot: () => next("boot").pipe(Effect.map(BootId.make)),
      incident: () => next("incident"),
    });
  }),
);

/** Scripted deterministic turn-executor Layer for durable runtime tests. */
export const TurnExecutorTestLayer = Layer.effectContext(
  Effect.gen(function* () {
    const events = yield* Ref.make<ReadonlyArray<AgentStreamEvent>>([]);
    const failure = yield* Ref.make<Option.Option<AgentRpcError>>(Option.none());
    const executions = yield* Ref.make<ReadonlyArray<TurnExecutionInput>>([]);
    const never = yield* Ref.make(false);
    const started = yield* Queue.unbounded<TurnExecutionInput>();
    const paused = yield* Ref.make(false);
    const releases = yield* Queue.unbounded<void>();
    const partials = yield* Ref.make<ReadonlyArray<PersistPartialInput>>([]);
    const assistantIds = yield* Ref.make<ReadonlySet<string>>(new Set());

    const service = TurnExecutorTest.of({
      execute: (input) =>
        Stream.unwrap(
          Effect.gen(function* () {
            yield* Ref.update(executions, (observed) => [...observed, input]);
            yield* Queue.offer(started, input);
            if (yield* Ref.get(paused)) yield* Queue.take(releases);
            const nextFailure = yield* Ref.get(failure);
            if (Option.isSome(nextFailure)) return Stream.fail(nextFailure.value);
            if (yield* Ref.get(never)) return Stream.never;
            return Stream.fromIterable(yield* Ref.get(events));
          }),
        ),
      persistPartial: Effect.fn("TurnExecutor.Test.persistPartial")(function* (input) {
        yield* Ref.update(partials, (observed) => [...observed, input]);
        yield* Ref.update(assistantIds, (current) => {
          const next = new Set(current);
          next.add(input.assistantMessageId);
          return next;
        });
      }),
      hasAssistantMessage: Effect.fn("TurnExecutor.Test.hasAssistantMessage")(function* (
        _sessionId: SessionId,
        assistantMessageId: MessageId,
      ) {
        return (yield* Ref.get(assistantIds)).has(assistantMessageId);
      }),
      setEvents: Effect.fn("TurnExecutor.Test.setEvents")(function* (script) {
        yield* Ref.set(events, script);
      }),
      setFailure: Effect.fn("TurnExecutor.Test.setFailure")(function* (error) {
        yield* Ref.set(failure, error);
      }),
      pauseExecutions: Effect.fn("TurnExecutor.Test.pauseExecutions")(function* () {
        yield* Ref.set(paused, true);
      }),
      releaseExecution: Effect.fn("TurnExecutor.Test.releaseExecution")(function* () {
        yield* Queue.offer(releases, undefined);
      }),
      setNever: Effect.fn("TurnExecutor.Test.setNever")(function* (enabled) {
        yield* Ref.set(never, enabled);
      }),
      nextExecution: Effect.fn("TurnExecutor.Test.nextExecution")(function* () {
        return yield* Queue.take(started);
      }),
      executions: Effect.fn("TurnExecutor.Test.executions")(function* () {
        return yield* Ref.get(executions);
      }),
      partials: Effect.fn("TurnExecutor.Test.partials")(function* () {
        return yield* Ref.get(partials);
      }),
    });

    return Context.empty().pipe(
      Context.add(TurnExecutor, service),
      Context.add(TurnExecutorTest, service),
    );
  }),
);
