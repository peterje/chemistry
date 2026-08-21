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
  updateContext,
  sendMessage,
  compactSession,
) {}
