import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Prompt from "effect/unstable/ai/Prompt";
import type * as Response from "effect/unstable/ai/Response";
import {
  AgentContext,
  AgentInferenceError,
  AgentStreamEvent,
  CompactionStats,
  SessionSnapshot,
  StreamMessage,
  StreamMessagePart,
  chatHistoryFromMessages,
  type ChatMessage,
  type MessageId,
  type SessionId,
} from "@chemistry/contracts/agent-protocol";
import type { StoredSession } from "./session-store.ts";

const decodeJson = Schema.decodeUnknownEffect(Schema.Json);

/** Default durable context assigned to a newly observed session. */
export const defaultAgentContext = AgentContext.make({
  systemPrompt:
    "You are a concise assistant. Use tools when they improve the answer and explain their results clearly.",
  memory: "",
});

/** Construct a new empty durable session. */
export const createInitialSession = (sessionId: SessionId): StoredSession => ({
  sessionId,
  context: defaultAgentContext,
  messages: [],
  compactions: [],
});

const contextText = (context: AgentContext): string =>
  context.memory.trim().length === 0
    ? context.systemPrompt
    : `${context.systemPrompt}\n\n<session-memory>\n${context.memory}\n</session-memory>`;

interface VisibleHistory {
  readonly summary: string | undefined;
  readonly messages: ReadonlyArray<ChatMessage>;
}

const visibleHistory = (session: StoredSession): VisibleHistory => {
  const overlay = session.compactions.at(-1);
  if (overlay === undefined) {
    return { summary: undefined, messages: session.messages };
  }
  const endIndex = session.messages.findIndex((message) => message.id === overlay.toMessageId);
  if (endIndex < 0) {
    return { summary: undefined, messages: session.messages };
  }
  return {
    summary: overlay.summary,
    messages: session.messages.slice(endIndex + 1),
  };
};

const modelMessages = (session: StoredSession): ReadonlyArray<Prompt.Message> => {
  const visible = visibleHistory(session);
  return [
    Prompt.systemMessage({ content: contextText(session.context) }),
    ...(visible.summary === undefined
      ? []
      : [
          Prompt.systemMessage({
            content: `<conversation-summary>\n${visible.summary}\n</conversation-summary>`,
          }),
        ]),
    ...visible.messages.map((entry) => entry.message),
  ];
};

/** Assemble the model prompt from durable context, summary, and recent history. */
export const assembleModelPrompt = (session: StoredSession): Prompt.Prompt =>
  Prompt.fromMessages(modelMessages(session));

/** Assemble a provider-valid prompt that continues a persisted partial assistant turn. */
export const assembleContinuationPrompt = (session: StoredSession): Prompt.Prompt =>
  Prompt.fromMessages([
    ...modelMessages(session),
    Prompt.userMessage({
      content: [
        Prompt.textPart({
          text: "Continue the previous assistant response exactly where it stopped. Do not repeat completed text.",
        }),
      ],
    }),
  ]);

const partSize = (part: Prompt.Part): number => {
  switch (part.type) {
    case "text":
    case "reasoning":
      return part.text.length;
    case "file":
      return (
        part.mediaType.length +
        (part.fileName?.length ?? 0) +
        (Predicate.isString(part.data)
          ? part.data.length
          : part.data instanceof URL
            ? part.data.href.length
            : part.data.byteLength)
      );
    case "tool-call":
      return part.name.length + (JSON.stringify(part.params)?.length ?? 0);
    case "tool-result":
      return part.name.length + (JSON.stringify(part.result)?.length ?? 0);
    case "tool-approval-request":
      return part.approvalId.length + part.toolCallId.length;
    case "tool-approval-response":
      return part.approvalId.length + (part.reason?.length ?? 0);
  }
};

const messageSize = (message: Prompt.Message): number =>
  message.role === "system"
    ? message.content.length
    : message.content.reduce((total, part) => total + partSize(part), 0);

/** Estimate model tokens with the Workers-safe four-characters-per-token rule. */
export const estimateModelTokens = (session: StoredSession): number => {
  const visible = visibleHistory(session);
  const contextCharacters = contextText(session.context).length;
  const summaryCharacters = visible.summary?.length ?? 0;
  const messageCharacters = visible.messages.reduce(
    (total, entry) => total + messageSize(entry.message),
    0,
  );
  return Math.ceil((contextCharacters + summaryCharacters + messageCharacters) / 4);
};

/** Derive observable compaction statistics from durable state. */
export const compactionStats = (session: StoredSession): CompactionStats => {
  const visible = visibleHistory(session);
  return CompactionStats.make({
    rawMessageCount: session.messages.length,
    modelMessageCount: visible.messages.length + (visible.summary === undefined ? 0 : 1),
    estimatedModelTokens: estimateModelTokens(session),
    compactionCount: session.compactions.length,
    lastCompactedAt:
      session.compactions.length === 0
        ? null
        : (session.compactions[session.compactions.length - 1]?.createdAt ?? null),
  });
};

/** Project a durable session into its public RPC snapshot. */
export const snapshot = (session: StoredSession): SessionSnapshot =>
  SessionSnapshot.make({
    sessionId: session.sessionId,
    context: session.context,
    chat: chatHistoryFromMessages(session.messages),
    compactions: session.compactions,
    stats: compactionStats(session),
  });

/** Construct a durable user text message backed by Effect AI Prompt. */
export function textMessage(
  id: MessageId,
  role: "user",
  text: string,
  createdAt: number,
): ChatMessage & { readonly message: Prompt.UserMessage };
/** Construct a durable assistant text message backed by Effect AI Prompt. */
export function textMessage(
  id: MessageId,
  role: "assistant",
  text: string,
  createdAt: number,
): ChatMessage & { readonly message: Prompt.AssistantMessage };
/** Construct a durable text message for the selected supported chat role. */
export function textMessage(
  id: MessageId,
  role: "user" | "assistant",
  text: string,
  createdAt: number,
): ChatMessage {
  return {
    id,
    createdAt,
    message:
      role === "user"
        ? Prompt.userMessage({ content: [Prompt.textPart({ text })] })
        : Prompt.assistantMessage({ content: [Prompt.textPart({ text })] }),
  };
}

/** Project one canonical chat message into the stable durable replay-event shape. */
export const streamMessage = (entry: ChatMessage): StreamMessage => {
  const { message } = entry;
  const parts =
    message.role === "system"
      ? [StreamMessagePart.cases.Text.make({ text: message.content })]
      : message.content.flatMap((part) => {
          switch (part.type) {
            case "text":
            case "reasoning":
              return [StreamMessagePart.cases.Text.make({ text: part.text })];
            case "file":
              return [
                StreamMessagePart.cases.Text.make({
                  text: `[file ${part.fileName ?? part.mediaType}]`,
                }),
              ];
            case "tool-call":
            case "tool-result":
            case "tool-approval-request":
            case "tool-approval-response":
              return [];
          }
        });
  return StreamMessage.make({
    id: entry.id,
    role: message.role,
    parts,
    createdAt: entry.createdAt,
  });
};

const inferenceDecodeError = (operation: string, cause: unknown) =>
  new AgentInferenceError({ operation, message: String(cause) });

/** One role-correct canonical message segment derived from a model response. */
export interface TranscriptSegment {
  /** Effect AI assistant or tool message ready for durable Prompt history. */
  readonly message: Prompt.AssistantMessage | Prompt.ToolMessage;
}

/** Group complete or streamed Effect AI response parts into canonical messages. */
export const transcriptSegmentsFromResponse = (
  parts: ReadonlyArray<Response.AnyPart>,
): Effect.Effect<ReadonlyArray<TranscriptSegment>, AgentInferenceError> =>
  Effect.succeed(
    Prompt.fromResponseParts(parts).content.flatMap((message) =>
      message.role === "assistant" || message.role === "tool" ? [{ message }] : [],
    ),
  );

/** Convert one streamed Effect AI response part into a live UI event. */
export const agentEventFromResponsePart = (
  part: Response.AnyPart,
): Effect.Effect<AgentStreamEvent | undefined, AgentInferenceError> => {
  switch (part.type) {
    case "text":
      return Effect.succeed(AgentStreamEvent.cases.TextDelta.make({ delta: part.text }));
    case "text-delta":
      return Effect.succeed(AgentStreamEvent.cases.TextDelta.make({ delta: part.delta }));
    case "tool-call":
      return decodeJson(part.params).pipe(
        Effect.map((input) =>
          AgentStreamEvent.cases.ToolCall.make({
            callId: part.id,
            name: part.name,
            input,
          }),
        ),
        Effect.mapError((cause) => inferenceDecodeError("decode-tool-call", cause)),
      );
    case "tool-result":
      return decodeJson(part.result).pipe(
        Effect.map((output) =>
          AgentStreamEvent.cases.ToolResult.make({
            callId: part.id,
            name: part.name,
            output,
            isFailure: part.isFailure,
          }),
        ),
        Effect.mapError((cause) => inferenceDecodeError("decode-tool-result", cause)),
      );
    default:
      return Effect.succeed(undefined);
  }
};
