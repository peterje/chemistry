import * as Schema from "effect/Schema";
import {
  BootId,
  DurableStreamEvent,
  MessageId,
  OperationId,
  RuntimeCheckpoint,
  RuntimeOperationStatus,
  RuntimeOperationSummary,
  RuntimeSnapshot,
  SessionId,
  StreamId,
  SubmissionId,
  type BootId as BootIdType,
  type OperationId as OperationIdType,
  type SessionId as SessionIdType,
} from "../shared/agent-protocol.ts";

const NonNegativeInt = Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0));
const Cursor = Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(-1));

/** Durable execution and replay limits applied by the runtime module. */
export interface RuntimeLimits {
  /** Maximum submissions waiting behind the active turn. */
  readonly maxQueuedTurns: number;
  /** Maximum persisted events retained in one stream. */
  readonly maxEventsPerStream: number;
  /** Maximum encoded bytes retained in one durable event. */
  readonly maxEventBytes: number;
  /** Maximum cumulative encoded bytes retained in one stream. */
  readonly maxStreamBytes: number;
  /** Maximum terminal streams retained per session. */
  readonly maxRetainedStreams: number;
  /** Milliseconds a completed stream remains replayable. */
  readonly completedStreamRetentionMs: number;
  /** Milliseconds before an execution ownership lease expires. */
  readonly leaseDurationMs: number;
  /** Maximum no-progress recovery attempts. */
  readonly maxRecoveryAttempts: number;
  /** Maximum cumulative recovery work units. */
  readonly maxRecoveryWork: number;
  /** Maximum classified memory-reset strikes. */
  readonly maxOomStrikes: number;
  /** Milliseconds without progress before recovery is exhausted. */
  readonly noProgressTimeoutMs: number;
  /** Minimum milliseconds between recovery alarm attempts. */
  readonly alarmDebounceMs: number;
  /** Milliseconds allowed to wait for serialized turn ownership to stabilize. */
  readonly stableStateTimeoutMs: number;
  /** Milliseconds allowed for a live inference event gap. */
  readonly stallTimeoutMs: number;
}

/** Conservative finite defaults for every durable runtime loop and buffer. */
export const defaultRuntimeLimits: RuntimeLimits = {
  maxQueuedTurns: 16,
  maxEventsPerStream: 2_048,
  maxEventBytes: 60 * 1_024,
  maxStreamBytes: 128 * 1_024,
  maxRetainedStreams: 8,
  completedStreamRetentionMs: 24 * 60 * 60 * 1_000,
  leaseDurationMs: 30_000,
  maxRecoveryAttempts: 5,
  maxRecoveryWork: 1_024,
  maxOomStrikes: 3,
  noProgressTimeoutMs: 120_000,
  alarmDebounceMs: 1_000,
  stableStateTimeoutMs: 30_000,
  stallTimeoutMs: 45_000,
};

/** Persisted operation record for one admitted logical turn. */
export const RuntimeOperationRecord = Schema.Struct({
  operationId: OperationId,
  submissionId: SubmissionId,
  streamId: StreamId,
  prompt: Schema.NonEmptyString.check(Schema.isMaxLength(32_000)),
  userMessageId: MessageId,
  assistantMessageId: MessageId,
  status: RuntimeOperationStatus,
  checkpoint: RuntimeCheckpoint,
  generation: NonNegativeInt,
  attempt: NonNegativeInt,
  progress: NonNegativeInt,
  recoveryWork: NonNegativeInt,
  ownerBootId: Schema.NullOr(BootId),
  leaseExpiresAt: Schema.NullOr(Schema.Number),
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
  terminalReason: Schema.NullOr(Schema.String),
}).annotate({ identifier: "RuntimeOperationRecord" });

/** A decoded persisted durable operation. */
export interface RuntimeOperationRecord extends Schema.Schema.Type<typeof RuntimeOperationRecord> {}

/** Persisted lifecycle states for a durable event stream. */
export const RuntimeStreamStatus = Schema.Literals(["open", "completed", "failed", "interrupted"]);

/** A decoded durable stream lifecycle state. */
export type RuntimeStreamStatus = typeof RuntimeStreamStatus.Type;

/** Persisted event log and terminal metadata for one logical turn stream. */
export const RuntimeStreamRecord = Schema.Struct({
  streamId: StreamId,
  operationId: OperationId,
  status: RuntimeStreamStatus,
  latestSequence: Cursor,
  encodedBytes: NonNegativeInt,
  events: Schema.Array(DurableStreamEvent),
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
  expiresAt: Schema.NullOr(Schema.Number),
  terminalReason: Schema.NullOr(Schema.String),
}).annotate({ identifier: "RuntimeStreamRecord" });

/** A decoded persisted durable stream. */
export interface RuntimeStreamRecord extends Schema.Schema.Type<typeof RuntimeStreamRecord> {}

/** Recovery classifications selected from durable checkpoints and stream evidence. */
export const RecoveryKind = Schema.Literals([
  "pre-stream-retry",
  "partial-continuation",
  "terminal-noop",
  "parked",
  "unrecoverable",
]);

/** A decoded recovery classification. */
export type RecoveryKind = typeof RecoveryKind.Type;

/** Persisted recovery-incident lifecycle states. */
export const RecoveryStatus = Schema.Literals([
  "detected",
  "scheduled",
  "attempting",
  "completed",
  "parked",
  "exhausted",
  "failed",
]);

/** A decoded recovery-incident state. */
export type RecoveryStatus = typeof RecoveryStatus.Type;

/** One durable recovery incident retained across wakes and alarms. */
export const RecoveryIncident = Schema.Struct({
  incidentId: Schema.NonEmptyString,
  operationId: OperationId,
  kind: RecoveryKind,
  status: RecoveryStatus,
  attempt: NonNegativeInt,
  progressBaseline: NonNegativeInt,
  recoveryWork: NonNegativeInt,
  oomStrikes: NonNegativeInt,
  firstSeenAt: Schema.Number,
  lastProgressAt: Schema.Number,
  scheduledAt: Schema.NullOr(Schema.Number),
  terminalReason: Schema.NullOr(Schema.String),
}).annotate({ identifier: "RecoveryIncident" });

/** A decoded durable recovery incident. */
export interface RecoveryIncident extends Schema.Schema.Type<typeof RecoveryIncident> {}

/** Bounded alarm work selected by the runtime. */
export const RuntimeAlarmIntent = Schema.TaggedUnion({
  Recover: {
    operationId: OperationId,
    generation: NonNegativeInt,
    scheduledAt: Schema.Number,
  },
  Cleanup: {
    scheduledAt: Schema.Number,
  },
});

/** A decoded runtime alarm intent. */
export type RuntimeAlarmIntent = typeof RuntimeAlarmIntent.Type;

/** Versioned durable runtime record stored independently from the transcript. */
export const RuntimeState = Schema.Struct({
  version: Schema.Literal(1),
  sessionId: SessionId,
  bootId: BootId,
  generation: NonNegativeInt,
  operations: Schema.Array(RuntimeOperationRecord),
  queue: Schema.Array(OperationId),
  activeOperationId: Schema.NullOr(OperationId),
  streams: Schema.Array(RuntimeStreamRecord),
  recovery: Schema.NullOr(RecoveryIncident),
  alarm: Schema.NullOr(RuntimeAlarmIntent),
  lastTerminalReason: Schema.NullOr(Schema.String),
  lastWakeAt: Schema.Number,
}).annotate({ identifier: "RuntimeState" });

/** A decoded version-one durable runtime record. */
export interface RuntimeState extends Schema.Schema.Type<typeof RuntimeState> {}

/** Build the empty runtime state used when migrating an existing session. */
export const initialRuntimeState = (
  sessionId: SessionIdType,
  bootId: BootIdType,
  now: number,
): RuntimeState =>
  RuntimeState.make({
    version: 1,
    sessionId,
    bootId,
    generation: 0,
    operations: [],
    queue: [],
    activeOperationId: null,
    streams: [],
    recovery: null,
    alarm: null,
    lastTerminalReason: null,
    lastWakeAt: now,
  });

/** Find one operation without exposing mutable persistence mechanics. */
export const findRuntimeOperation = (
  state: RuntimeState,
  operationId: OperationIdType,
): RuntimeOperationRecord | undefined =>
  state.operations.find((operation) => operation.operationId === operationId);

/** Project durable runtime state into the browser-safe diagnostic snapshot. */
export const runtimeSnapshot = (state: RuntimeState): RuntimeSnapshot => {
  const active =
    state.activeOperationId === null
      ? undefined
      : findRuntimeOperation(state, state.activeOperationId);
  const activeOperation =
    active === undefined
      ? null
      : RuntimeOperationSummary.make({
          operationId: active.operationId,
          submissionId: active.submissionId,
          streamId: active.streamId,
          status: active.status,
          checkpoint: active.checkpoint,
          generation: active.generation,
          attempt: active.attempt,
          progress: active.progress,
          recoveryWork: active.recoveryWork,
          terminalReason: active.terminalReason,
          updatedAt: active.updatedAt,
        });
  return RuntimeSnapshot.make({
    bootId: state.bootId,
    activeOperation,
    queueDepth: state.queue.length,
    retainedStreamCount: state.streams.length,
    recoveryAttempt: state.recovery?.attempt ?? 0,
    lastTerminalReason: state.lastTerminalReason,
  });
};
