import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Prompt from "effect/unstable/ai/Prompt";
import type * as Response from "effect/unstable/ai/Response";
import {
  AgentContext,
  AgentInferenceError,
  AgentStreamEvent,
  CompactionStats,
  SessionSnapshot,
  TranscriptMessage,
  TranscriptPart,
  type MessageId,
  type SessionId,
} from "../shared/agent-protocol.ts";
import { StoredSession } from "./session-store.ts";

const decodeJson = Schema.decodeUnknownEffect(Schema.Json);

/** Default durable context assigned to a newly observed session. */
export const defaultAgentContext = AgentContext.make({
  systemPrompt:
    "You are a concise assistant. Use tools when they improve the answer and explain their results clearly.",
  memory: "",
});

/** Construct a new empty durable session. */
export const createInitialSession = (sessionId: SessionId): StoredSession =>
  StoredSession.make({
    sessionId,
    context: defaultAgentContext,
    messages: [],
    compactions: [],
  });

const contextText = (context: AgentContext): string =>
  context.memory.trim().length === 0
    ? context.systemPrompt
    : `${context.systemPrompt}\n\n<session-memory>\n${context.memory}\n</session-memory>`;

const promptPart = (part: TranscriptPart): Prompt.AssistantMessagePart =>
  TranscriptPart.match<Prompt.AssistantMessagePart>(part, {
    Text: ({ text }) => Prompt.textPart({ text }),
    ToolCall: ({ callId, name, input }) =>
      Prompt.toolCallPart({
        id: callId,
        name,
        params: input,
        providerExecuted: false,
      }),
    ToolResult: ({ callId, name, output, isFailure }) =>
      Prompt.toolResultPart({
        id: callId,
        name,
        result: output,
        isFailure,
        providerExecuted: false,
      }),
  });

const toPromptMessage = (message: TranscriptMessage): Prompt.Message => {
  switch (message.role) {
    case "system": {
      const text: Array<string> = [];
      for (const part of message.parts) {
        if (TranscriptPart.guards.Text(part)) text.push(part.text);
      }
      return Prompt.systemMessage({ content: text.join("\n") });
    }
    case "user": {
      const content: Array<Prompt.TextPart> = [];
      for (const part of message.parts) {
        if (TranscriptPart.guards.Text(part)) {
          content.push(Prompt.textPart({ text: part.text }));
        }
      }
      return Prompt.userMessage({ content });
    }
    case "assistant":
      return Prompt.assistantMessage({
        content: message.parts.map(promptPart),
      });
    case "tool": {
      const content: Array<Prompt.ToolResultPart> = [];
      for (const part of message.parts) {
        if (TranscriptPart.guards.ToolResult(part)) {
          content.push(
            Prompt.toolResultPart({
              id: part.callId,
              name: part.name,
              result: part.output,
              isFailure: part.isFailure,
              providerExecuted: false,
            }),
          );
        }
      }
      return Prompt.toolMessage({ content });
    }
  }
};

interface VisibleHistory {
  readonly summary: string | undefined;
  readonly messages: ReadonlyArray<TranscriptMessage>;
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
    ...visible.messages.map(toPromptMessage),
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

const partSize = (part: TranscriptPart): number =>
  TranscriptPart.match(part, {
    Text: ({ text }) => text.length,
    ToolCall: ({ name, input }) => name.length + JSON.stringify(input).length,
    ToolResult: ({ name, output }) => name.length + JSON.stringify(output).length,
  });

/** Estimate model tokens with the Workers-safe four-characters-per-token rule. */
export const estimateModelTokens = (session: StoredSession): number => {
  const visible = visibleHistory(session);
  const contextCharacters = contextText(session.context).length;
  const summaryCharacters = visible.summary?.length ?? 0;
  const messageCharacters = visible.messages.reduce(
    (total, message) =>
      total + message.parts.reduce((partTotal, part) => partTotal + partSize(part), 0),
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
    ...session,
    stats: compactionStats(session),
  });

/** Construct a durable text message. */
export const textMessage = (
  id: MessageId,
  role: "user" | "assistant",
  text: string,
  createdAt: number,
): TranscriptMessage =>
  TranscriptMessage.make({
    id,
    role,
    parts: [TranscriptPart.cases.Text.make({ text })],
    createdAt,
  });

const inferenceDecodeError = (operation: string, cause: unknown) =>
  new AgentInferenceError({ operation, message: String(cause) });

const transcriptPartFromPromptPart = (
  part: Prompt.Part,
): Effect.Effect<TranscriptPart | undefined, AgentInferenceError> => {
  switch (part.type) {
    case "text":
      return Effect.succeed(TranscriptPart.cases.Text.make({ text: part.text }));
    case "tool-call":
      return decodeJson(part.params).pipe(
        Effect.map((input) =>
          TranscriptPart.cases.ToolCall.make({
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
          TranscriptPart.cases.ToolResult.make({
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

/** One role-correct transcript message segment derived from a model response. */
export interface TranscriptSegment {
  readonly role: "assistant" | "tool";
  readonly parts: ReadonlyArray<TranscriptPart>;
}

const transcriptSegmentFromPromptMessage = (
  message: Prompt.Message,
): Effect.Effect<TranscriptSegment | undefined, AgentInferenceError> => {
  if (message.role !== "assistant" && message.role !== "tool") {
    return Effect.succeed(undefined);
  }
  return Effect.forEach(message.content, transcriptPartFromPromptPart).pipe(
    Effect.map((parts) => {
      const present: Array<TranscriptPart> = [];
      for (const part of parts) {
        if (part !== undefined) present.push(part);
      }
      return present.length === 0 ? undefined : { role: message.role, parts: present };
    }),
  );
};

/** Group complete or streamed Effect AI response parts into durable messages. */
export const transcriptSegmentsFromResponse = (
  parts: ReadonlyArray<Response.AnyPart>,
): Effect.Effect<ReadonlyArray<TranscriptSegment>, AgentInferenceError> =>
  Effect.forEach(Prompt.fromResponseParts(parts).content, transcriptSegmentFromPromptMessage).pipe(
    Effect.map((segments) => {
      const present: Array<TranscriptSegment> = [];
      for (const segment of segments) {
        if (segment !== undefined) present.push(segment);
      }
      return present;
    }),
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
