import * as Schema from "effect/Schema";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcSchema from "effect/unstable/rpc/RpcSchema";

const NonNegativeInt = Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0));
const PositiveInt = Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0));

/** A durable conversation identifier selected by the client. */
export const SessionId = Schema.NonEmptyString.check(
  Schema.isMaxLength(64),
  Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_-]*$/),
).pipe(Schema.brand("SessionId"));

/** The branded type decoded by {@link SessionId}. */
export type SessionId = typeof SessionId.Type;

/** A client-selected idempotency key for one logical turn submission. */
export const SubmissionId = Schema.NonEmptyString.check(Schema.isMaxLength(128)).pipe(
  Schema.brand("SubmissionId"),
);

/** The branded type decoded by {@link SubmissionId}. */
export type SubmissionId = typeof SubmissionId.Type;

/** A durable execution identifier assigned to one admitted operation. */
export const OperationId = Schema.NonEmptyString.check(Schema.isMaxLength(128)).pipe(
  Schema.brand("OperationId"),
);

/** The branded type decoded by {@link OperationId}. */
export type OperationId = typeof OperationId.Type;

/** A durable stream identifier assigned to one logical turn. */
export const StreamId = Schema.NonEmptyString.check(Schema.isMaxLength(128)).pipe(
  Schema.brand("StreamId"),
);

/** The branded type decoded by {@link StreamId}. */
export type StreamId = typeof StreamId.Type;

/** A hibernatable browser connection identifier. */
export const ConnectionId = Schema.NonEmptyString.check(Schema.isMaxLength(128)).pipe(
  Schema.brand("ConnectionId"),
);

/** The branded type decoded by {@link ConnectionId}. */
export type ConnectionId = typeof ConnectionId.Type;

/** A boot identifier proving which Durable Object isolate handled an event. */
export const BootId = Schema.NonEmptyString.check(Schema.isMaxLength(128)).pipe(
  Schema.brand("BootId"),
);

/** The branded type decoded by {@link BootId}. */
export type BootId = typeof BootId.Type;

/** A stable identifier for one stored transcript message. */
export const MessageId = Schema.NonEmptyString.pipe(Schema.brand("MessageId"));

/** The branded type decoded by {@link MessageId}. */
export type MessageId = typeof MessageId.Type;

/** The roles represented in durable conversation history. */
export const MessageRole = Schema.Literals(["system", "user", "assistant", "tool"]);

/** A durable conversation-message role. */
export type MessageRole = typeof MessageRole.Type;

/** Serializable content parts retained in the raw transcript. */
export const TranscriptPart = Schema.TaggedUnion({
  Text: {
    text: Schema.String,
  },
  ToolCall: {
    callId: Schema.String,
    name: Schema.String,
    input: Schema.Json,
  },
  ToolResult: {
    callId: Schema.String,
    name: Schema.String,
    output: Schema.Json,
    isFailure: Schema.Boolean,
  },
});

/** A text, tool-call, or tool-result transcript part. */
export type TranscriptPart = typeof TranscriptPart.Type;

/** A message in the immutable, user-visible session transcript. */
export const TranscriptMessage = Schema.Struct({
  id: MessageId,
  role: MessageRole,
  parts: Schema.Array(TranscriptPart),
  createdAt: Schema.Number,
}).annotate({ identifier: "TranscriptMessage" });

/** A decoded immutable transcript message. */
export interface TranscriptMessage extends Schema.Schema.Type<typeof TranscriptMessage> {}

/** Durable instructions and memory injected into every model turn. */
export const AgentContext = Schema.Struct({
  systemPrompt: Schema.NonEmptyString,
  memory: Schema.String,
}).annotate({ identifier: "AgentContext" });

/** Decoded per-session agent context. */
export interface AgentContext extends Schema.Schema.Type<typeof AgentContext> {}

/** One non-destructive summary overlay over a raw transcript range. */
export const Compaction = Schema.Struct({
  id: Schema.NonEmptyString,
  fromMessageId: MessageId,
  toMessageId: MessageId,
  summary: Schema.NonEmptyString,
  sourceMessageCount: PositiveInt,
  createdAt: Schema.Number,
}).annotate({ identifier: "Compaction" });

/** A decoded persisted compaction overlay. */
export interface Compaction extends Schema.Schema.Type<typeof Compaction> {}

/** Observable raw-history and model-context size information. */
export const CompactionStats = Schema.Struct({
  rawMessageCount: NonNegativeInt,
  modelMessageCount: NonNegativeInt,
  estimatedModelTokens: NonNegativeInt,
  compactionCount: NonNegativeInt,
  lastCompactedAt: Schema.NullOr(Schema.Number),
}).annotate({ identifier: "CompactionStats" });

/** Decoded compaction statistics. */
export interface CompactionStats extends Schema.Schema.Type<typeof CompactionStats> {}

/** Complete durable state rendered by the browser for one session. */
export const SessionSnapshot = Schema.Struct({
  sessionId: SessionId,
  context: AgentContext,
  messages: Schema.Array(TranscriptMessage),
  compactions: Schema.Array(Compaction),
  stats: CompactionStats,
}).annotate({ identifier: "SessionSnapshot" });

/** A decoded session snapshot. */
export interface SessionSnapshot extends Schema.Schema.Type<typeof SessionSnapshot> {}

/** Result returned after a manual or automatic compaction attempt. */
export const CompactionResult = Schema.Struct({
  compacted: Schema.Boolean,
  reason: Schema.Literals(["manual", "threshold", "below-threshold", "insufficient-history"]),
  stats: CompactionStats,
}).annotate({ identifier: "CompactionResult" });

/** A decoded compaction result. */
export interface CompactionResult extends Schema.Schema.Type<typeof CompactionResult> {}

/** Stream events emitted during a message turn. */
export const AgentStreamEvent = Schema.TaggedUnion({
  TurnStarted: {
    userMessage: TranscriptMessage,
  },
  TextDelta: {
    delta: Schema.String,
  },
  ToolCall: {
    callId: Schema.String,
    name: Schema.String,
    input: Schema.Json,
  },
  ToolResult: {
    callId: Schema.String,
    name: Schema.String,
    output: Schema.Json,
    isFailure: Schema.Boolean,
  },
  CompactionCompleted: {
    result: CompactionResult,
  },
  TurnCompleted: {
    assistantMessage: TranscriptMessage,
    stats: CompactionStats,
  },
});

/** A decoded event in a streamed agent turn. */
export type AgentStreamEvent = typeof AgentStreamEvent.Type;

/** Lifecycle states exposed by one durable operation. */
export const RuntimeOperationStatus = Schema.Literals([
  "queued",
  "running",
  "interrupted",
  "recovering",
  "parked",
  "completed",
  "failed",
]);

/** A decoded durable-operation lifecycle state. */
export type RuntimeOperationStatus = typeof RuntimeOperationStatus.Type;

/** Explicit checkpoints from which a durable turn can be classified or resumed. */
export const RuntimeCheckpoint = Schema.Literals([
  "admitted",
  "preparing",
  "streaming",
  "partial-persisted",
  "parked",
  "terminal",
]);

/** A decoded durable-operation checkpoint. */
export type RuntimeCheckpoint = typeof RuntimeCheckpoint.Type;

/** Public diagnostic summary for one durable operation. */
export const RuntimeOperationSummary = Schema.Struct({
  operationId: OperationId,
  submissionId: SubmissionId,
  streamId: StreamId,
  status: RuntimeOperationStatus,
  checkpoint: RuntimeCheckpoint,
  generation: NonNegativeInt,
  attempt: NonNegativeInt,
  progress: NonNegativeInt,
  recoveryWork: NonNegativeInt,
  terminalReason: Schema.NullOr(Schema.String),
  updatedAt: Schema.Number,
}).annotate({ identifier: "RuntimeOperationSummary" });

/** A decoded public durable-operation summary. */
export interface RuntimeOperationSummary extends Schema.Schema.Type<
  typeof RuntimeOperationSummary
> {}

/** Runtime diagnostics included in snapshots and resume probes. */
export const RuntimeSnapshot = Schema.Struct({
  bootId: BootId,
  activeOperation: Schema.NullOr(RuntimeOperationSummary),
  queueDepth: NonNegativeInt,
  retainedStreamCount: NonNegativeInt,
  recoveryAttempt: NonNegativeInt,
  lastTerminalReason: Schema.NullOr(Schema.String),
}).annotate({ identifier: "RuntimeSnapshot" });

/** A decoded runtime diagnostic snapshot. */
export interface RuntimeSnapshot extends Schema.Schema.Type<typeof RuntimeSnapshot> {}

/** One append-before-publish event in a durable turn stream. */
export const DurableStreamEvent = Schema.Struct({
  streamId: StreamId,
  operationId: OperationId,
  sequence: NonNegativeInt,
  event: AgentStreamEvent,
  producedAt: Schema.Number,
}).annotate({ identifier: "DurableStreamEvent" });

/** A decoded durable stream event. */
export interface DurableStreamEvent extends Schema.Schema.Type<typeof DurableStreamEvent> {}

/** Browser-to-runtime frames accepted by the hibernatable WebSocket protocol. */
export const RuntimeClientFrame = Schema.TaggedUnion({
  ResumeAck: {
    probeId: Schema.NonEmptyString,
    streamId: Schema.NullOr(StreamId),
    afterSequence: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(-1)),
  },
  SubmitTurn: {
    submissionId: SubmissionId,
    prompt: Schema.NonEmptyString.check(Schema.isMaxLength(32_000)),
  },
  StreamAck: {
    streamId: StreamId,
    sequence: NonNegativeInt,
  },
  Ping: {
    nonce: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  },
  KeepAlive: {},
});

/** A decoded browser-to-runtime WebSocket frame. */
export type RuntimeClientFrame = typeof RuntimeClientFrame.Type;

/** Runtime-to-browser frames emitted by the hibernatable WebSocket protocol. */
export const RuntimeServerFrame = Schema.TaggedUnion({
  ResumeProbe: {
    probeId: Schema.NonEmptyString,
    connectionId: ConnectionId,
    sessionId: SessionId,
    activeStreamId: Schema.NullOr(StreamId),
    latestSequence: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(-1)),
    runtime: RuntimeSnapshot,
  },
  TurnAccepted: {
    submissionId: SubmissionId,
    operationId: OperationId,
    streamId: StreamId,
    queuePosition: NonNegativeInt,
  },
  StreamEvent: {
    durableEvent: DurableStreamEvent,
    replay: Schema.Boolean,
  },
  ResumeComplete: {
    streamId: Schema.NullOr(StreamId),
    throughSequence: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(-1)),
  },
  Recovering: {
    operation: RuntimeOperationSummary,
  },
  StreamTerminal: {
    streamId: StreamId,
    operationId: OperationId,
    status: Schema.Literals(["completed", "failed", "interrupted"]),
    sequence: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(-1)),
    generation: NonNegativeInt,
    attempt: NonNegativeInt,
    recoveryWork: NonNegativeInt,
    reason: Schema.NullOr(Schema.String),
  },
  ProtocolError: {
    code: Schema.Literals([
      "invalid-frame",
      "frame-too-large",
      "stale-probe",
      "stale-stream",
      "queue-full",
      "runtime-unavailable",
    ]),
    message: Schema.String,
    recoverable: Schema.Boolean,
  },
  Pong: {
    nonce: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
    bootId: BootId,
  },
  KeepAliveAck: {},
});

/** A decoded runtime-to-browser WebSocket frame. */
export type RuntimeServerFrame = typeof RuntimeServerFrame.Type;

/** A WebSocket frame failed shared protocol decoding or state validation. */
export class AgentProtocolError extends Schema.TaggedError<AgentProtocolError>()(
  "AgentProtocolError",
  {
    code: Schema.String,
    message: Schema.String,
  },
) {}

/** A Durable Object storage operation failed. */
export class AgentPersistenceError extends Schema.TaggedError<AgentPersistenceError>()(
  "AgentPersistenceError",
  {
    operation: Schema.String,
    message: Schema.String,
  },
) {}

/** Native Workers AI inference failed. */
export class AgentInferenceError extends Schema.TaggedError<AgentInferenceError>()(
  "AgentInferenceError",
  {
    operation: Schema.String,
    message: Schema.String,
  },
) {}

/** A requested or automatic compaction failed. */
export class AgentCompactionError extends Schema.TaggedError<AgentCompactionError>()(
  "AgentCompactionError",
  {
    message: Schema.String,
  },
) {}

/** The model exhausted the configured tool-call step budget. */
export class ToolLoopLimitExceeded extends Schema.TaggedError<ToolLoopLimitExceeded>()(
  "ToolLoopLimitExceeded",
  {
    maxSteps: PositiveInt,
    message: Schema.String,
  },
) {}

/** Typed errors that can cross the public agent RPC boundary. */
export const AgentRpcError = Schema.Union([
  AgentPersistenceError,
  AgentInferenceError,
  AgentCompactionError,
  ToolLoopLimitExceeded,
]);

/** A typed public RPC failure. */
export type AgentRpcError = typeof AgentRpcError.Type;

const getSession = Rpc.make("getSession", {
  payload: { sessionId: SessionId },
  success: SessionSnapshot,
  error: AgentRpcError,
});

const getRuntime = Rpc.make("getRuntime", {
  payload: { sessionId: SessionId },
  success: RuntimeSnapshot,
  error: AgentRpcError,
});

const updateContext = Rpc.make("updateContext", {
  payload: {
    sessionId: SessionId,
    context: AgentContext,
  },
  success: SessionSnapshot,
  error: AgentRpcError,
});

const sendMessage = Rpc.make("sendMessage", {
  payload: {
    sessionId: SessionId,
    prompt: Schema.NonEmptyString.check(Schema.isMaxLength(32_000)),
  },
  success: RpcSchema.Stream(AgentStreamEvent, AgentRpcError),
});

const compactSession = Rpc.make("compactSession", {
  payload: { sessionId: SessionId },
  success: CompactionResult,
  error: AgentRpcError,
});

/** Shared Effect RPC contract imported by browser, Worker, and Durable Object. */
export class AgentRpcs extends RpcGroup.make(
  getSession,
  getRuntime,
  updateContext,
  sendMessage,
  compactSession,
) {}
