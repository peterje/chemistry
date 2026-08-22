import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import {
  AgentInferenceError,
  DurableStreamEvent,
  MessageId,
  type AgentRpcError,
  type AgentStreamEvent,
  type DurableStreamEvent as DurableStreamEventType,
  type OperationId,
  type RuntimeSnapshot,
  type SessionId,
  type StreamId,
  type SubmissionId,
} from "../shared/agent-protocol.ts";
import { RuntimeIdSource } from "./runtime-id-source.ts";
import {
  RecoveryIncident,
  RuntimeOperationRecord,
  RuntimeStreamRecord,
  defaultRuntimeLimits,
  findRuntimeOperation,
  initialRuntimeState,
  runtimeSnapshot,
  type RecoveryKind,
  type RuntimeLimits,
  type RuntimeOperationRecord as RuntimeOperationRecordType,
  type RuntimeState as RuntimeStateType,
  type RuntimeStreamRecord as RuntimeStreamRecordType,
} from "./runtime-state.ts";
import {
  RuntimeCapacityError,
  RuntimeFenceError,
  RuntimePersistenceError,
  RuntimeStore,
  RuntimeTransitionError,
} from "./runtime-store.ts";
import { TurnExecutor, type TurnExecutionInput } from "./turn-executor.ts";

/** All expected failures exposed by the durable execution module. */
export type DurableExecutionError =
  | AgentRpcError
  | RuntimeCapacityError
  | RuntimeFenceError
  | RuntimePersistenceError
  | RuntimeTransitionError;

/** Result of idempotently admitting one turn into the durable FIFO. */
export interface RuntimeAdmission {
  /** Complete persisted operation identity and checkpoint. */
  readonly operation: RuntimeOperationRecordType;
  /** Zero-based position after the active turn. */
  readonly queuePosition: number;
  /** Whether the submission already existed. */
  readonly duplicate: boolean;
}

/** Stored replay result used by reconnect and duplicate callers. */
export interface RuntimeReplay {
  /** Durable stream being replayed. */
  readonly streamId: StreamId;
  /** Events strictly after the requested high-water sequence. */
  readonly events: ReadonlyArray<DurableStreamEventType>;
  /** Highest durable stream sequence, or -1 for an empty stream. */
  readonly latestSequence: number;
  /** Current persisted stream lifecycle. */
  readonly status: RuntimeStreamRecordType["status"];
  /** Persisted terminal reason when present. */
  readonly terminalReason: string | null;
}

/** Resume-probe information derived entirely from durable state. */
export interface RuntimeProbe {
  /** Active or most recently retained stream. */
  readonly streamId: StreamId | null;
  /** Highest durable sequence for the selected stream. */
  readonly latestSequence: number;
  /** Browser-safe runtime diagnostics. */
  readonly snapshot: RuntimeSnapshot;
}

/** Result of reconciling a new Durable Object boot with durable ownership. */
export interface RuntimeWakeResult {
  /** Runtime diagnostics after wake reconciliation. */
  readonly snapshot: RuntimeSnapshot;
  /** Epoch time at which a recovery alarm should run. */
  readonly recoveryAlarmAt: number | null;
  /** Operation requiring recovery, when one was detected. */
  readonly recoverableOperationId: OperationId | null;
  /** Oldest durably queued operation that lost its ephemeral runner. */
  readonly runnableOperationId: OperationId | null;
  /** Stream owned by the oldest durably queued operation. */
  readonly runnableStreamId: StreamId | null;
}

type ClaimResult =
  | {
      readonly _tag: "Terminal";
      readonly operation: RuntimeOperationRecordType;
    }
  | {
      readonly _tag: "Claimed";
      readonly operation: RuntimeOperationRecordType;
    };

type RecoveryPreparation =
  | {
      readonly _tag: "Exhausted";
      readonly operation: RuntimeOperationRecordType;
    }
  | {
      readonly _tag: "Parked";
      readonly operation: RuntimeOperationRecordType;
    }
  | {
      readonly _tag: "Attempt";
      readonly operation: RuntimeOperationRecordType;
      readonly kind: RecoveryKind;
      readonly partial: string;
    };

/** Terminal stream metadata sent independently from replayable content events. */
export interface RuntimeTerminal {
  /** Durable stream identity. */
  readonly streamId: StreamId;
  /** Durable operation identity. */
  readonly operationId: OperationId;
  /** Terminal or interrupted stream state. */
  readonly status: "completed" | "failed" | "interrupted";
  /** Highest durable sequence. */
  readonly sequence: number;
  /** Final fenced operation generation. */
  readonly generation: number;
  /** Final bounded recovery attempt count. */
  readonly attempt: number;
  /** Final bounded recovery work count. */
  readonly recoveryWork: number;
  /** Safe terminal reason. */
  readonly reason: string | null;
  /** Epoch time after which this retained stream may be removed. */
  readonly cleanupAt: number | null;
}

/** Deep durable execution interface consumed by RPC and WebSocket adapters. */
export interface DurableExecutionOperations {
  /** Idempotently admit one bounded turn submission. */
  readonly admit: (
    sessionId: SessionId,
    prompt: string,
    submissionId?: SubmissionId,
  ) => Effect.Effect<RuntimeAdmission, DurableExecutionError>;
  /** Claim and execute an admitted turn, persisting each event before emission. */
  readonly run: (
    sessionId: SessionId,
    operationId: OperationId,
  ) => Stream.Stream<DurableStreamEventType, DurableExecutionError>;
  /** Replay events strictly after a client high-water sequence. */
  readonly replay: (
    sessionId: SessionId,
    streamId: StreamId,
    afterSequence: number,
  ) => Effect.Effect<RuntimeReplay, DurableExecutionError>;
  /** Inspect active stream and runtime diagnostics for a resume probe. */
  readonly probe: (sessionId: SessionId) => Effect.Effect<RuntimeProbe, DurableExecutionError>;
  /** Read terminal metadata for one retained stream. */
  readonly terminal: (
    sessionId: SessionId,
    streamId: StreamId,
  ) => Effect.Effect<RuntimeTerminal | null, DurableExecutionError>;
  /** Reconcile boot ownership and schedule orphan recovery before new work. */
  readonly wake: (sessionId: SessionId) => Effect.Effect<RuntimeWakeResult, DurableExecutionError>;
  /** Execute one bounded recovery attempt for the active incident. */
  readonly recover: (
    sessionId: SessionId,
  ) => Stream.Stream<DurableStreamEventType, DurableExecutionError>;
  /** Remove expired terminal streams in a bounded batch. */
  readonly cleanup: (sessionId: SessionId) => Effect.Effect<number, DurableExecutionError>;
}

/** Effect service owning durable admission, execution, replay, and recovery policy. */
export class DurableExecution extends Context.Service<
  DurableExecution,
  DurableExecutionOperations
>()("@chemistry/DurableExecution") {}

const eventEncoder = new TextEncoder();

const transitionError = (operation: string, message: string) =>
  new RuntimeTransitionError({ operation, message });

const replaceOperation = (
  state: RuntimeStateType,
  operation: RuntimeOperationRecordType,
): ReadonlyArray<RuntimeOperationRecordType> =>
  state.operations.map((candidate) =>
    candidate.operationId === operation.operationId ? operation : candidate,
  );

const replaceStream = (
  state: RuntimeStateType,
  stream: RuntimeStreamRecordType,
): ReadonlyArray<RuntimeStreamRecordType> =>
  state.streams.map((candidate) => (candidate.streamId === stream.streamId ? stream : candidate));

const findStream = (
  state: RuntimeStateType,
  streamId: StreamId,
): RuntimeStreamRecordType | undefined =>
  state.streams.find((stream) => stream.streamId === streamId);

const queuePosition = (state: RuntimeStateType, operationId: OperationId): number => {
  if (state.activeOperationId === operationId) return 0;
  const index = state.queue.findIndex((queuedId) => queuedId === operationId);
  return index < 0 ? 0 : index + (state.activeOperationId === null ? 0 : 1);
};

const classifyRecovery = (
  operation: RuntimeOperationRecordType,
  stream: RuntimeStreamRecordType,
): RecoveryKind => {
  if (operation.status === "parked" || operation.checkpoint === "parked") return "parked";
  if (operation.status === "completed" || operation.status === "failed") return "terminal-noop";
  if (stream.status === "completed" || stream.status === "failed") return "terminal-noop";
  if (stream.latestSequence < 0) return "pre-stream-retry";
  if (operation.checkpoint === "streaming" || operation.checkpoint === "partial-persisted") {
    return "partial-continuation";
  }
  return "unrecoverable";
};

const partialText = (stream: RuntimeStreamRecordType): string => {
  let text = "";
  for (const item of stream.events) {
    if (item.event._tag === "TextDelta") text += item.event.delta;
  }
  return text;
};

const toTurnInput = (
  operation: RuntimeOperationRecordType,
  sessionId: SessionId,
  mode: "fresh" | "continue",
): TurnExecutionInput => ({
  sessionId,
  operationId: operation.operationId,
  submissionId: operation.submissionId,
  streamId: operation.streamId,
  prompt: operation.prompt,
  userMessageId: operation.userMessageId,
  assistantMessageId: operation.assistantMessageId,
  mode,
});

const errorMessage = (error: DurableExecutionError): string => error.message;

const isInferenceStall = (error: DurableExecutionError): boolean =>
  error._tag === "AgentInferenceError" && error.operation === "inference-stall";

const isMemoryLimitReset = (error: DurableExecutionError): boolean =>
  error.message.toLowerCase().includes("exceeded its memory limit");

/** Construct the live durable execution Layer with finite runtime limits. */
export const makeDurableExecutionLayer = (
  limits: RuntimeLimits = defaultRuntimeLimits,
): Layer.Layer<DurableExecution, never, RuntimeStore | RuntimeIdSource | TurnExecutor> =>
  Layer.effect(
    DurableExecution,
    Effect.gen(function* () {
      const store = yield* RuntimeStore;
      const ids = yield* RuntimeIdSource;
      const executor = yield* TurnExecutor;
      const bootId = yield* ids.boot();
      const turnSemaphore = yield* Semaphore.make(1);
      const takeTurnPermit = turnSemaphore.take(1).pipe(
        Effect.timeoutOrElse({
          duration: limits.stableStateTimeoutMs,
          orElse: () =>
            Effect.fail(
              transitionError(
                "stable-state",
                `Turn ownership did not stabilize within ${limits.stableStateTimeoutMs}ms`,
              ),
            ),
        }),
      );

      const initial = (sessionId: SessionId, now: number) =>
        initialRuntimeState(sessionId, bootId, now);

      const load = Effect.fn("DurableExecution.load")(function* (sessionId: SessionId) {
        const now = yield* Clock.currentTimeMillis;
        return yield* store.load(sessionId, initial(sessionId, now));
      });

      const admit = Effect.fn("DurableExecution.admit")(function* (
        sessionId: SessionId,
        prompt: string,
        suppliedSubmissionId?: SubmissionId,
      ) {
        const now = yield* Clock.currentTimeMillis;
        const submissionId = suppliedSubmissionId ?? (yield* ids.submission());
        const operationId = yield* ids.operation();
        const streamId = yield* ids.stream();
        const userMessageId = MessageId.make(`${operationId}:user`);
        const assistantMessageId = MessageId.make(`${operationId}:assistant`);
        return yield* store.transact<RuntimeAdmission, RuntimeCapacityError>(
          sessionId,
          initial(sessionId, now),
          Effect.fn("DurableExecution.admit.transition")(function* (state) {
            const existing = state.operations.find(
              (operation) => operation.submissionId === submissionId,
            );
            if (existing !== undefined) {
              return {
                state,
                value: {
                  operation: existing,
                  queuePosition: queuePosition(state, existing.operationId),
                  duplicate: true,
                } satisfies RuntimeAdmission,
              };
            }
            if (state.queue.length >= limits.maxQueuedTurns) {
              return yield* new RuntimeCapacityError({
                resource: "turn-queue",
                limit: limits.maxQueuedTurns,
                message: `Turn queue is limited to ${limits.maxQueuedTurns} submissions`,
              });
            }
            const operation = RuntimeOperationRecord.make({
              operationId,
              submissionId,
              streamId,
              prompt,
              userMessageId,
              assistantMessageId,
              status: "queued",
              checkpoint: "admitted",
              generation: 0,
              attempt: 0,
              progress: 0,
              recoveryWork: 0,
              ownerBootId: null,
              leaseExpiresAt: null,
              createdAt: now,
              updatedAt: now,
              terminalReason: null,
            });
            const stream = RuntimeStreamRecord.make({
              streamId,
              operationId,
              status: "open",
              latestSequence: -1,
              encodedBytes: 0,
              events: [],
              createdAt: now,
              updatedAt: now,
              expiresAt: null,
              terminalReason: null,
            });
            const next: RuntimeStateType = {
              ...state,
              operations: [...state.operations, operation],
              queue: [...state.queue, operationId],
              streams: [...state.streams, stream],
            };
            return {
              state: next,
              value: {
                operation,
                queuePosition: queuePosition(next, operationId),
                duplicate: false,
              } satisfies RuntimeAdmission,
            };
          }),
        );
      });

      const claim = Effect.fn("DurableExecution.claim")(function* (
        sessionId: SessionId,
        operationId: OperationId,
        mode: "fresh" | "continue",
      ) {
        const now = yield* Clock.currentTimeMillis;
        return yield* store.transact<ClaimResult, RuntimeTransitionError>(
          sessionId,
          initial(sessionId, now),
          Effect.fn("DurableExecution.claim.transition")(function* (state) {
            const operation = findRuntimeOperation(state, operationId);
            if (operation === undefined) {
              return yield* transitionError("claim", `Operation ${operationId} does not exist`);
            }
            if (operation.status === "completed" || operation.status === "failed") {
              return { state, value: { _tag: "Terminal" as const, operation } };
            }
            if (state.activeOperationId !== null && state.activeOperationId !== operationId) {
              return yield* transitionError(
                "claim",
                `Operation ${state.activeOperationId} already owns the session`,
              );
            }
            if (
              mode === "fresh" &&
              state.activeOperationId === null &&
              state.queue[0] !== operationId
            ) {
              return yield* transitionError("claim", `Operation ${operationId} is not FIFO head`);
            }
            const generation = state.generation + 1;
            const claimed: RuntimeOperationRecordType = {
              ...operation,
              status: mode === "continue" ? "recovering" : "running",
              checkpoint: mode === "continue" ? "partial-persisted" : "preparing",
              generation,
              attempt: mode === "continue" ? operation.attempt + 1 : operation.attempt,
              ownerBootId: bootId,
              leaseExpiresAt: now + limits.leaseDurationMs,
              updatedAt: now,
            };
            const next: RuntimeStateType = {
              ...state,
              bootId,
              generation,
              operations: replaceOperation(state, claimed),
              queue: state.queue.filter((queuedId) => queuedId !== operationId),
              activeOperationId: operationId,
              lastWakeAt: now,
            };
            return { state: next, value: { _tag: "Claimed" as const, operation: claimed } };
          }),
        );
      });

      const markStreaming = Effect.fn("DurableExecution.markStreaming")(function* (
        sessionId: SessionId,
        operation: RuntimeOperationRecordType,
      ) {
        const now = yield* Clock.currentTimeMillis;
        return yield* store.transact(
          sessionId,
          initial(sessionId, now),
          Effect.fn("DurableExecution.markStreaming.transition")(function* (state) {
            const current = findRuntimeOperation(state, operation.operationId);
            if (current === undefined) {
              return yield* transitionError("mark-streaming", "Operation disappeared");
            }
            if (current.generation !== operation.generation) {
              return yield* new RuntimeFenceError({
                operationId: operation.operationId,
                expectedGeneration: current.generation,
                receivedGeneration: operation.generation,
                message: "Stale owner cannot begin streaming",
              });
            }
            const updated: RuntimeOperationRecordType = {
              ...current,
              status: "running",
              checkpoint: "streaming",
              updatedAt: now,
              leaseExpiresAt: now + limits.leaseDurationMs,
            };
            return {
              state: { ...state, operations: replaceOperation(state, updated) },
              value: updated,
            };
          }),
        );
      });

      const append = Effect.fn("DurableExecution.append")(function* (
        sessionId: SessionId,
        operation: RuntimeOperationRecordType,
        event: AgentStreamEvent,
      ) {
        const now = yield* Clock.currentTimeMillis;
        return yield* store.transact(
          sessionId,
          initial(sessionId, now),
          Effect.fn("DurableExecution.append.transition")(function* (state) {
            const current = findRuntimeOperation(state, operation.operationId);
            const stream = findStream(state, operation.streamId);
            if (current === undefined || stream === undefined) {
              return yield* transitionError("append", "Operation or stream disappeared");
            }
            if (current.generation !== operation.generation) {
              return yield* new RuntimeFenceError({
                operationId: operation.operationId,
                expectedGeneration: current.generation,
                receivedGeneration: operation.generation,
                message: "Stale owner cannot append a stream event",
              });
            }
            if (stream.events.length >= limits.maxEventsPerStream) {
              return yield* new RuntimeCapacityError({
                resource: "stream-events",
                limit: limits.maxEventsPerStream,
                message: `Stream is limited to ${limits.maxEventsPerStream} events`,
              });
            }
            if (current.attempt > 0 && current.recoveryWork >= limits.maxRecoveryWork) {
              return yield* new RuntimeCapacityError({
                resource: "recovery-work",
                limit: limits.maxRecoveryWork,
                message: `Recovery is limited to ${limits.maxRecoveryWork} events`,
              });
            }
            const durableEvent = DurableStreamEvent.make({
              streamId: stream.streamId,
              operationId: operation.operationId,
              sequence: stream.latestSequence + 1,
              event,
              producedAt: now,
            });
            const encodedBytes = eventEncoder.encode(JSON.stringify(durableEvent)).byteLength;
            if (encodedBytes > limits.maxEventBytes) {
              return yield* new RuntimeCapacityError({
                resource: "stream-event-bytes",
                limit: limits.maxEventBytes,
                message: `Durable stream event is limited to ${limits.maxEventBytes} bytes`,
              });
            }
            if (stream.encodedBytes + encodedBytes > limits.maxStreamBytes) {
              return yield* new RuntimeCapacityError({
                resource: "stream-bytes",
                limit: limits.maxStreamBytes,
                message: `Durable stream is limited to ${limits.maxStreamBytes} encoded bytes`,
              });
            }
            const updatedOperation: RuntimeOperationRecordType = {
              ...current,
              status: "running",
              checkpoint: "streaming",
              progress: current.progress + 1,
              recoveryWork: current.attempt > 0 ? current.recoveryWork + 1 : current.recoveryWork,
              leaseExpiresAt: now + limits.leaseDurationMs,
              updatedAt: now,
            };
            const updatedStream: RuntimeStreamRecordType = {
              ...stream,
              status: "open",
              latestSequence: durableEvent.sequence,
              encodedBytes: stream.encodedBytes + encodedBytes,
              events: [...stream.events, durableEvent],
              updatedAt: now,
              expiresAt: null,
              terminalReason: null,
            };
            const nextRecovery =
              state.recovery === null
                ? null
                : {
                    ...state.recovery,
                    lastProgressAt: now,
                    recoveryWork: updatedOperation.recoveryWork,
                  };
            return {
              state: {
                ...state,
                operations: replaceOperation(state, updatedOperation),
                streams: replaceStream(state, updatedStream),
                recovery: nextRecovery,
              },
              value: durableEvent,
            };
          }),
        );
      });

      const settle = Effect.fn("DurableExecution.settle")(function* (
        sessionId: SessionId,
        operation: RuntimeOperationRecordType,
        outcome: "completed" | "failed",
        reason: string | null,
      ) {
        const now = yield* Clock.currentTimeMillis;
        return yield* store.transact(
          sessionId,
          initial(sessionId, now),
          Effect.fn("DurableExecution.settle.transition")(function* (state) {
            const current = findRuntimeOperation(state, operation.operationId);
            const stream = findStream(state, operation.streamId);
            if (current === undefined || stream === undefined) {
              return yield* transitionError("settle", "Operation or stream disappeared");
            }
            if (
              current.status === "completed" ||
              current.status === "failed" ||
              stream.status === "completed" ||
              stream.status === "failed"
            ) {
              return {
                state,
                value: {
                  streamId: stream.streamId,
                  operationId: current.operationId,
                  status: stream.status === "completed" ? "completed" : "failed",
                  sequence: stream.latestSequence,
                  generation: current.generation,
                  attempt: current.attempt,
                  recoveryWork: current.recoveryWork,
                  reason: stream.terminalReason,
                  cleanupAt: stream.expiresAt,
                } satisfies RuntimeTerminal,
              };
            }
            if (current.generation !== operation.generation) {
              return yield* new RuntimeFenceError({
                operationId: operation.operationId,
                expectedGeneration: current.generation,
                receivedGeneration: operation.generation,
                message: "Stale owner cannot terminalize an operation",
              });
            }
            const updatedOperation: RuntimeOperationRecordType = {
              ...current,
              status: outcome,
              checkpoint: "terminal",
              leaseExpiresAt: null,
              ownerBootId: null,
              terminalReason: reason,
              updatedAt: now,
            };
            const updatedStream: RuntimeStreamRecordType = {
              ...stream,
              status: outcome,
              expiresAt: now + limits.completedStreamRetentionMs,
              terminalReason: reason,
              updatedAt: now,
            };
            const terminal: RuntimeTerminal = {
              streamId: stream.streamId,
              operationId: current.operationId,
              status: outcome,
              sequence: stream.latestSequence,
              generation: updatedOperation.generation,
              attempt: updatedOperation.attempt,
              recoveryWork: updatedOperation.recoveryWork,
              reason,
              cleanupAt: updatedStream.expiresAt,
            };
            return {
              state: {
                ...state,
                operations: replaceOperation(state, updatedOperation),
                streams: replaceStream(state, updatedStream),
                activeOperationId: null,
                recovery: null,
                alarm: null,
                lastTerminalReason: reason,
              },
              value: terminal,
            };
          }),
        );
      });

      const scheduleRecoveryAfterFailure = Effect.fn(
        "DurableExecution.scheduleRecoveryAfterFailure",
      )(function* (
        sessionId: SessionId,
        operation: RuntimeOperationRecordType,
        error: DurableExecutionError,
        recoveredPartial: boolean,
      ) {
        const now = yield* Clock.currentTimeMillis;
        const incidentId = yield* ids.incident();
        return yield* store.transact(
          sessionId,
          initial(sessionId, now),
          Effect.fn("DurableExecution.scheduleRecoveryAfterFailure.transition")(function* (state) {
            const current = findRuntimeOperation(state, operation.operationId);
            const stream = findStream(state, operation.streamId);
            if (current === undefined || stream === undefined) {
              return yield* transitionError("schedule-recovery", "Operation or stream disappeared");
            }
            if (current.generation !== operation.generation) {
              return yield* new RuntimeFenceError({
                operationId: operation.operationId,
                expectedGeneration: current.generation,
                receivedGeneration: operation.generation,
                message: "Stale owner cannot schedule recovery",
              });
            }
            const scheduledAt = now + limits.alarmDebounceMs;
            const kind = classifyRecovery(current, stream);
            const existing = state.recovery;
            const oomStrikes = (existing?.oomStrikes ?? 0) + (isMemoryLimitReset(error) ? 1 : 0);
            const incident = RecoveryIncident.make({
              incidentId: existing?.incidentId ?? incidentId,
              operationId: current.operationId,
              kind,
              status: "scheduled",
              attempt: existing?.attempt ?? current.attempt,
              progressBaseline: existing?.progressBaseline ?? current.progress,
              recoveryWork: current.recoveryWork,
              oomStrikes,
              firstSeenAt: existing?.firstSeenAt ?? now,
              lastProgressAt: existing?.lastProgressAt ?? now,
              scheduledAt,
              terminalReason: null,
            });
            const interrupted: RuntimeOperationRecordType = {
              ...current,
              status: "interrupted",
              checkpoint: recoveredPartial ? "partial-persisted" : current.checkpoint,
              ownerBootId: null,
              leaseExpiresAt: null,
              updatedAt: now,
              terminalReason: error.message,
            };
            const interruptedStream: RuntimeStreamRecordType = {
              ...stream,
              status: "interrupted",
              updatedAt: now,
              terminalReason: error.message,
            };
            return {
              state: {
                ...state,
                operations: replaceOperation(state, interrupted),
                streams: replaceStream(state, interruptedStream),
                activeOperationId: current.operationId,
                recovery: incident,
                alarm: {
                  _tag: "Recover",
                  operationId: current.operationId,
                  generation: current.generation,
                  scheduledAt,
                },
              },
              value: scheduledAt,
            };
          }),
        );
      });

      const replay = Effect.fn("DurableExecution.replay")(function* (
        sessionId: SessionId,
        streamId: StreamId,
        afterSequence: number,
      ) {
        if (!Number.isInteger(afterSequence) || afterSequence < -1) {
          return yield* transitionError("replay", "Replay cursor must be an integer at least -1");
        }
        const state = yield* load(sessionId);
        const stream = findStream(state, streamId);
        if (stream === undefined) {
          return yield* transitionError("replay", `Stream ${streamId} is not retained`);
        }
        return {
          streamId,
          events: stream.events.filter((event) => event.sequence > afterSequence),
          latestSequence: stream.latestSequence,
          status: stream.status,
          terminalReason: stream.terminalReason,
        } satisfies RuntimeReplay;
      });

      const terminal = Effect.fn("DurableExecution.terminal")(function* (
        sessionId: SessionId,
        streamId: StreamId,
      ) {
        const state = yield* load(sessionId);
        const stream = findStream(state, streamId);
        if (
          stream === undefined ||
          (stream.status !== "completed" &&
            stream.status !== "failed" &&
            stream.status !== "interrupted")
        ) {
          return null;
        }
        const operation = findRuntimeOperation(state, stream.operationId);
        if (operation === undefined) {
          return yield* transitionError("terminal", "Terminal operation record is missing");
        }
        return {
          streamId,
          operationId: stream.operationId,
          status: stream.status,
          sequence: stream.latestSequence,
          generation: operation.generation,
          attempt: operation.attempt,
          recoveryWork: operation.recoveryWork,
          reason: stream.terminalReason,
          cleanupAt: stream.expiresAt,
        } satisfies RuntimeTerminal;
      });

      const probe = Effect.fn("DurableExecution.probe")(function* (sessionId: SessionId) {
        const state = yield* load(sessionId);
        const activeStream =
          state.activeOperationId === null
            ? undefined
            : state.streams.find((stream) => stream.operationId === state.activeOperationId);
        const selected = activeStream ?? state.streams[state.streams.length - 1];
        return {
          streamId: selected?.streamId ?? null,
          latestSequence: selected?.latestSequence ?? -1,
          snapshot: runtimeSnapshot(state),
        } satisfies RuntimeProbe;
      });

      const executeClaimed = (
        sessionId: SessionId,
        operation: RuntimeOperationRecordType,
        mode: "fresh" | "continue",
      ): Stream.Stream<DurableStreamEventType, DurableExecutionError> =>
        Stream.unwrap(
          Effect.gen(function* () {
            const streaming = yield* markStreaming(sessionId, operation);
            const source = executor.execute(toTurnInput(streaming, sessionId, mode)).pipe(
              Stream.timeoutOrElse({
                duration: limits.stallTimeoutMs,
                orElse: () =>
                  Stream.fail(
                    new AgentInferenceError({
                      operation: "inference-stall",
                      message: `Inference produced no event for ${limits.stallTimeoutMs}ms`,
                    }),
                  ),
              }),
              Stream.mapEffect((event) => append(sessionId, streaming, event)),
              Stream.onEnd(settle(sessionId, streaming, "completed", null)),
              Stream.catchIf(
                (_error): _error is DurableExecutionError => true,
                (error) => {
                  const recoverable =
                    streaming.attempt > 0 || isInferenceStall(error) || isMemoryLimitReset(error);
                  return Stream.unwrap(
                    recoverable
                      ? scheduleRecoveryAfterFailure(
                          sessionId,
                          streaming,
                          error,
                          mode === "continue",
                        ).pipe(Effect.map(() => Stream.empty))
                      : settle(sessionId, streaming, "failed", errorMessage(error)).pipe(
                          Effect.map(() => Stream.fail(error)),
                        ),
                  );
                },
              ),
            );
            return source;
          }),
        );

      const runUnlocked = (
        sessionId: SessionId,
        operationId: OperationId,
        mode: "fresh" | "continue",
      ): Stream.Stream<DurableStreamEventType, DurableExecutionError> =>
        Stream.unwrap(
          claim(sessionId, operationId, mode).pipe(
            Effect.map((result) => {
              if (result._tag === "Terminal") {
                return Stream.unwrap(
                  replay(sessionId, result.operation.streamId, -1).pipe(
                    Effect.map((window) => Stream.fromIterable(window.events)),
                  ),
                );
              }
              return executeClaimed(sessionId, result.operation, mode);
            }),
          ),
        );

      const run = (
        sessionId: SessionId,
        operationId: OperationId,
      ): Stream.Stream<DurableStreamEventType, DurableExecutionError> =>
        Stream.scoped(
          Stream.unwrap(
            Effect.acquireRelease(takeTurnPermit, () => turnSemaphore.release(1)).pipe(
              Effect.map(() => runUnlocked(sessionId, operationId, "fresh")),
            ),
          ),
        );

      const wake = Effect.fn("DurableExecution.wake")(function* (sessionId: SessionId) {
        const now = yield* Clock.currentTimeMillis;
        const incidentId = yield* ids.incident();
        return yield* store.transact<RuntimeWakeResult, RuntimeTransitionError>(
          sessionId,
          initial(sessionId, now),
          Effect.fn("DurableExecution.wake.transition")(function* (state) {
            if (state.activeOperationId === null) {
              const queuedOperationId = state.queue[0] ?? null;
              const queuedOperation =
                queuedOperationId === null
                  ? undefined
                  : findRuntimeOperation(state, queuedOperationId);
              if (queuedOperationId !== null && queuedOperation === undefined) {
                return yield* transitionError("wake", "Queued operation record is missing");
              }
              const awakened: RuntimeStateType = { ...state, bootId, lastWakeAt: now };
              return {
                state: awakened,
                value: {
                  snapshot: runtimeSnapshot(awakened),
                  recoveryAlarmAt: null,
                  recoverableOperationId: null,
                  runnableOperationId: queuedOperationId,
                  runnableStreamId: queuedOperation?.streamId ?? null,
                } satisfies RuntimeWakeResult,
              };
            }
            const operation = findRuntimeOperation(state, state.activeOperationId);
            if (operation === undefined) {
              return yield* transitionError("wake", "Active operation record is missing");
            }
            const stream = findStream(state, operation.streamId);
            if (stream === undefined) {
              return yield* transitionError("wake", "Active stream record is missing");
            }
            if (operation.ownerBootId === bootId) {
              const sameBoot: RuntimeStateType = { ...state, lastWakeAt: now };
              return {
                state: sameBoot,
                value: {
                  snapshot: runtimeSnapshot(sameBoot),
                  recoveryAlarmAt: state.recovery?.scheduledAt ?? null,
                  recoverableOperationId: state.recovery?.operationId ?? null,
                  runnableOperationId: null,
                  runnableStreamId: null,
                } satisfies RuntimeWakeResult,
              };
            }
            if (
              state.recovery !== null &&
              state.recovery.operationId === operation.operationId &&
              state.recovery.status !== "completed" &&
              state.recovery.status !== "exhausted" &&
              state.recovery.status !== "failed"
            ) {
              const parked = state.recovery.status === "parked";
              const scheduledAt = parked
                ? null
                : (state.recovery.scheduledAt ?? now + limits.alarmDebounceMs);
              const interruptedOperation: RuntimeOperationRecordType = {
                ...operation,
                status: parked ? "parked" : "interrupted",
                ownerBootId: null,
                leaseExpiresAt: null,
                updatedAt: now,
              };
              const interruptedStream: RuntimeStreamRecordType = {
                ...stream,
                status: "interrupted",
                updatedAt: now,
              };
              const resumed: RuntimeStateType = {
                ...state,
                bootId,
                operations: replaceOperation(state, interruptedOperation),
                streams: replaceStream(state, interruptedStream),
                recovery: {
                  ...state.recovery,
                  status: parked ? "parked" : "scheduled",
                  scheduledAt,
                },
                alarm:
                  scheduledAt === null
                    ? null
                    : {
                        _tag: "Recover",
                        operationId: operation.operationId,
                        generation: operation.generation,
                        scheduledAt,
                      },
                lastWakeAt: now,
              };
              return {
                state: resumed,
                value: {
                  snapshot: runtimeSnapshot(resumed),
                  recoveryAlarmAt: scheduledAt,
                  recoverableOperationId: operation.operationId,
                  runnableOperationId: null,
                  runnableStreamId: null,
                } satisfies RuntimeWakeResult,
              };
            }
            const kind = classifyRecovery(operation, stream);
            if (kind === "terminal-noop") {
              const queuedOperationId = state.queue[0] ?? null;
              const queuedOperation =
                queuedOperationId === null
                  ? undefined
                  : findRuntimeOperation(state, queuedOperationId);
              if (queuedOperationId !== null && queuedOperation === undefined) {
                return yield* transitionError("wake", "Queued operation record is missing");
              }
              const terminalState: RuntimeStateType = {
                ...state,
                bootId,
                activeOperationId: null,
                lastWakeAt: now,
              };
              return {
                state: terminalState,
                value: {
                  snapshot: runtimeSnapshot(terminalState),
                  recoveryAlarmAt: null,
                  recoverableOperationId: null,
                  runnableOperationId: queuedOperationId,
                  runnableStreamId: queuedOperation?.streamId ?? null,
                } satisfies RuntimeWakeResult,
              };
            }
            const interruptedOperation: RuntimeOperationRecordType = {
              ...operation,
              status: kind === "parked" ? "parked" : "interrupted",
              ownerBootId: null,
              leaseExpiresAt: null,
              updatedAt: now,
              terminalReason: null,
            };
            const interruptedStream: RuntimeStreamRecordType = {
              ...stream,
              status: "interrupted",
              updatedAt: now,
            };
            const scheduledAt = now + limits.alarmDebounceMs;
            const incident = RecoveryIncident.make({
              incidentId,
              operationId: operation.operationId,
              kind,
              status: kind === "parked" ? "parked" : "scheduled",
              attempt: 0,
              progressBaseline: operation.progress,
              recoveryWork: operation.recoveryWork,
              oomStrikes: 0,
              firstSeenAt: now,
              lastProgressAt: now,
              scheduledAt: kind === "parked" ? null : scheduledAt,
              terminalReason: null,
            });
            const awakened: RuntimeStateType = {
              ...state,
              bootId,
              operations: replaceOperation(state, interruptedOperation),
              streams: replaceStream(state, interruptedStream),
              recovery: incident,
              alarm:
                kind === "parked"
                  ? null
                  : {
                      _tag: "Recover",
                      operationId: operation.operationId,
                      generation: operation.generation,
                      scheduledAt,
                    },
              lastWakeAt: now,
            };
            return {
              state: awakened,
              value: {
                snapshot: runtimeSnapshot(awakened),
                recoveryAlarmAt: kind === "parked" ? null : scheduledAt,
                recoverableOperationId: operation.operationId,
                runnableOperationId: null,
                runnableStreamId: null,
              } satisfies RuntimeWakeResult,
            };
          }),
        );
      });

      const reconcileTranscriptTerminal = Effect.fn("DurableExecution.reconcileTranscriptTerminal")(
        function* (sessionId: SessionId) {
          const now = yield* Clock.currentTimeMillis;
          const state = yield* store.load(sessionId, initial(sessionId, now));
          if (state.recovery === null) return false;
          const operation = findRuntimeOperation(state, state.recovery.operationId);
          if (operation === undefined || operation.checkpoint !== "streaming") return false;
          const stream = findStream(state, operation.streamId);
          if (stream === undefined) return false;
          if (stream.events.some((event) => event.event._tag === "ToolCall")) return false;
          if (!(yield* executor.hasAssistantMessage(sessionId, operation.assistantMessageId))) {
            return false;
          }
          return yield* store.transact(
            sessionId,
            initial(sessionId, now),
            Effect.fn("DurableExecution.reconcileTranscriptTerminal.transition")(
              function* (currentState) {
                const current = findRuntimeOperation(currentState, operation.operationId);
                const currentStream = findStream(currentState, operation.streamId);
                if (current === undefined || currentStream === undefined) {
                  return yield* transitionError(
                    "reconcile-terminal",
                    "Operation or stream disappeared",
                  );
                }
                if (current.status === "completed") {
                  return { state: currentState, value: true };
                }
                const generation = currentState.generation + 1;
                const completedOperation: RuntimeOperationRecordType = {
                  ...current,
                  status: "completed",
                  checkpoint: "terminal",
                  generation,
                  ownerBootId: null,
                  leaseExpiresAt: null,
                  terminalReason: null,
                  updatedAt: now,
                };
                const completedStream: RuntimeStreamRecordType = {
                  ...currentStream,
                  status: "completed",
                  terminalReason: null,
                  expiresAt: now + limits.completedStreamRetentionMs,
                  updatedAt: now,
                };
                return {
                  state: {
                    ...currentState,
                    generation,
                    operations: replaceOperation(currentState, completedOperation),
                    streams: replaceStream(currentState, completedStream),
                    activeOperationId: null,
                    recovery: null,
                    alarm: null,
                  },
                  value: true,
                };
              },
            ),
          );
        },
      );

      const prepareRecovery = Effect.fn("DurableExecution.prepareRecovery")(function* (
        sessionId: SessionId,
      ) {
        const now = yield* Clock.currentTimeMillis;
        return yield* store.transact<RecoveryPreparation, RuntimeTransitionError>(
          sessionId,
          initial(sessionId, now),
          Effect.fn("DurableExecution.prepareRecovery.transition")(function* (state) {
            const incident = state.recovery;
            if (incident === null) {
              return yield* transitionError("recover", "No recovery incident is scheduled");
            }
            const operation = findRuntimeOperation(state, incident.operationId);
            if (operation === undefined) {
              return yield* transitionError("recover", "Recovery operation is missing");
            }
            const stream = findStream(state, operation.streamId);
            if (stream === undefined) {
              return yield* transitionError("recover", "Recovery stream is missing");
            }
            if (
              incident.status === "exhausted" ||
              incident.status === "failed" ||
              incident.status === "completed"
            ) {
              return {
                state,
                value: { _tag: "Exhausted" as const, operation },
              };
            }
            const noProgressExpired = now - incident.lastProgressAt >= limits.noProgressTimeoutMs;
            const exhausted =
              incident.attempt >= limits.maxRecoveryAttempts ||
              incident.recoveryWork >= limits.maxRecoveryWork ||
              incident.oomStrikes >= limits.maxOomStrikes ||
              noProgressExpired;
            if (exhausted || incident.kind === "unrecoverable") {
              const reason =
                incident.kind === "unrecoverable"
                  ? "unrecoverable-checkpoint"
                  : incident.recoveryWork >= limits.maxRecoveryWork
                    ? "recovery-work-exhausted"
                    : incident.oomStrikes >= limits.maxOomStrikes
                      ? "recovery-oom-exhausted"
                      : noProgressExpired
                        ? "recovery-no-progress-timeout"
                        : "recovery-attempts-exhausted";
              const generation = state.generation + 1;
              const exhaustedOperation: RuntimeOperationRecordType = {
                ...operation,
                status: "failed",
                checkpoint: "terminal",
                generation,
                ownerBootId: null,
                leaseExpiresAt: null,
                terminalReason: reason,
                updatedAt: now,
              };
              const exhaustedStream: RuntimeStreamRecordType = {
                ...stream,
                status: "failed",
                terminalReason: reason,
                expiresAt: now + limits.completedStreamRetentionMs,
                updatedAt: now,
              };
              return {
                state: {
                  ...state,
                  generation,
                  operations: replaceOperation(state, exhaustedOperation),
                  streams: replaceStream(state, exhaustedStream),
                  activeOperationId: null,
                  recovery: {
                    ...incident,
                    status: "exhausted",
                    terminalReason: reason,
                    scheduledAt: null,
                  },
                  alarm: null,
                  lastTerminalReason: reason,
                },
                value: { _tag: "Exhausted" as const, operation: exhaustedOperation },
              };
            }
            if (incident.kind === "parked") {
              return { state, value: { _tag: "Parked" as const, operation } };
            }
            const generation = state.generation + 1;
            const recovering: RuntimeOperationRecordType = {
              ...operation,
              status: "recovering",
              generation,
              attempt: operation.attempt + 1,
              ownerBootId: bootId,
              leaseExpiresAt: now + limits.leaseDurationMs,
              updatedAt: now,
            };
            const attempting = {
              ...incident,
              status: "attempting" as const,
              attempt: incident.attempt + 1,
              scheduledAt: null,
            };
            return {
              state: {
                ...state,
                bootId,
                generation,
                operations: replaceOperation(state, recovering),
                streams: replaceStream(state, { ...stream, status: "open", updatedAt: now }),
                activeOperationId: operation.operationId,
                recovery: attempting,
                alarm: null,
              },
              value: {
                _tag: "Attempt" as const,
                operation: recovering,
                kind: incident.kind,
                partial: partialText(stream),
              },
            };
          }),
        );
      });

      const recover = (sessionId: SessionId) =>
        Stream.scoped(
          Stream.unwrap(
            Effect.acquireRelease(takeTurnPermit, () => turnSemaphore.release(1)).pipe(
              Effect.flatMap(() => reconcileTranscriptTerminal(sessionId)),
              Effect.flatMap((reconciled) =>
                reconciled
                  ? Effect.succeed(Stream.empty)
                  : prepareRecovery(sessionId).pipe(
                      Effect.flatMap((decision) => {
                        if (decision._tag === "Exhausted" || decision._tag === "Parked") {
                          return Effect.succeed(Stream.empty);
                        }
                        if (decision.kind === "partial-continuation") {
                          return executor
                            .persistPartial({
                              sessionId,
                              assistantMessageId: decision.operation.assistantMessageId,
                              text: decision.partial,
                            })
                            .pipe(
                              Effect.map(() =>
                                executeClaimed(sessionId, decision.operation, "continue"),
                              ),
                            );
                        }
                        if (decision.kind === "pre-stream-retry") {
                          return Effect.succeed(
                            executeClaimed(sessionId, decision.operation, "fresh"),
                          );
                        }
                        return Effect.succeed(Stream.empty);
                      }),
                    ),
              ),
            ),
          ),
        );

      const cleanup = Effect.fn("DurableExecution.cleanup")(function* (sessionId: SessionId) {
        const now = yield* Clock.currentTimeMillis;
        return yield* store.transact(
          sessionId,
          initial(sessionId, now),
          Effect.fn("DurableExecution.cleanup.transition")((state) =>
            Effect.sync(() => {
              const terminal = state.streams
                .filter(
                  (stream) =>
                    stream.streamId !==
                      state.streams.find(
                        (candidate) => candidate.operationId === state.activeOperationId,
                      )?.streamId &&
                    stream.status !== "open" &&
                    stream.status !== "interrupted",
                )
                .sort((left, right) => right.updatedAt - left.updatedAt);
              const retainedIds = new Set(
                terminal.slice(0, limits.maxRetainedStreams).map((stream) => stream.streamId),
              );
              const nextStreams = state.streams.filter(
                (stream) =>
                  stream.status === "open" ||
                  stream.status === "interrupted" ||
                  (retainedIds.has(stream.streamId) &&
                    (stream.expiresAt === null || stream.expiresAt > now)),
              );
              const retainedStreamIds = new Set(nextStreams.map((stream) => stream.streamId));
              const removedIds = new Set<OperationId>();
              for (const stream of state.streams) {
                if (!retainedStreamIds.has(stream.streamId)) {
                  removedIds.add(stream.operationId);
                }
              }
              const next: RuntimeStateType = {
                ...state,
                streams: nextStreams,
                operations: state.operations.filter(
                  (operation) => !removedIds.has(operation.operationId),
                ),
              };
              return {
                state: next,
                value: state.streams.length - nextStreams.length,
              };
            }),
          ),
        );
      });

      return DurableExecution.of({
        admit,
        run,
        replay,
        probe,
        terminal,
        wake,
        recover,
        cleanup,
      });
    }),
  );

/** Default live durable execution Layer. */
export const DurableExecutionLive = makeDurableExecutionLayer();
