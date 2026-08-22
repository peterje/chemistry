import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Prompt from "effect/unstable/ai/Prompt";
import {
  AgentContext,
  AgentPersistenceError,
  ChatHistory,
  Compaction,
  MessageId,
  SessionId,
  chatHistoryFromMessages,
  chatMessages,
  type ChatMessage,
} from "@chemistry/contracts/agent-protocol";

/** Application session state from which model context and UI snapshots are derived. */
export interface StoredSession {
  /** Durable session identity. */
  readonly sessionId: SessionId;
  /** Mutable model instructions and memory. */
  readonly context: AgentContext;
  /** Metadata-bearing view over canonical Effect AI prompt messages. */
  readonly messages: ReadonlyArray<ChatMessage>;
  /** Non-destructive summary overlays over raw message identities. */
  readonly compactions: ReadonlyArray<Compaction>;
}

/** Versioned storage record whose chat payload is Effect AI's canonical `Prompt`. */
export const PersistedSession = Schema.Struct({
  version: Schema.Literal(2),
  sessionId: SessionId,
  context: AgentContext,
  chat: ChatHistory,
  compactions: Schema.Array(Compaction),
}).annotate({ identifier: "PersistedSession" });

/** Decoded version-two session storage record. */
export interface PersistedSession extends Schema.Schema.Type<typeof PersistedSession> {}

const LegacyTranscriptPart = Schema.TaggedUnion({
  Text: { text: Schema.String },
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

type LegacyTranscriptPart = typeof LegacyTranscriptPart.Type;

const LegacyTranscriptMessage = Schema.Struct({
  id: MessageId,
  role: Schema.Literals(["system", "user", "assistant", "tool"]),
  parts: Schema.Array(LegacyTranscriptPart),
  createdAt: Schema.Number,
});

/** Session storage shape written before canonical Effect AI Prompt persistence. */
export const LegacyStoredSession = Schema.Struct({
  sessionId: SessionId,
  context: AgentContext,
  messages: Schema.Array(LegacyTranscriptMessage),
  compactions: Schema.Array(Compaction),
}).annotate({ identifier: "LegacyStoredSession" });

/** Decoded legacy session storage record. */
export interface LegacyStoredSession extends Schema.Schema.Type<typeof LegacyStoredSession> {}

const assistantPartFromLegacy = (part: LegacyTranscriptPart): Prompt.AssistantMessagePart =>
  LegacyTranscriptPart.match<Prompt.AssistantMessagePart>(part, {
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

const promptMessageFromLegacy = (
  message: LegacyStoredSession["messages"][number],
): Prompt.Message => {
  switch (message.role) {
    case "system":
      return Prompt.systemMessage({
        content: message.parts
          .filter(LegacyTranscriptPart.guards.Text)
          .map((part) => part.text)
          .join("\n"),
      });
    case "user":
      return Prompt.userMessage({
        content: message.parts
          .filter(LegacyTranscriptPart.guards.Text)
          .map(({ text }) => Prompt.textPart({ text })),
      });
    case "assistant":
      return Prompt.assistantMessage({ content: message.parts.map(assistantPartFromLegacy) });
    case "tool":
      return Prompt.toolMessage({
        content: message.parts.filter(LegacyTranscriptPart.guards.ToolResult).map((part) =>
          Prompt.toolResultPart({
            id: part.callId,
            name: part.name,
            result: part.output,
            isFailure: part.isFailure,
            providerExecuted: false,
          }),
        ),
      });
  }
};

/** Convert a persisted Prompt record into application session state. */
export const storedSessionFromPersisted = (persisted: PersistedSession): StoredSession => ({
  sessionId: persisted.sessionId,
  context: persisted.context,
  messages: chatMessages(persisted.chat),
  compactions: persisted.compactions,
});

/** Convert application session state into the versioned canonical Prompt record. */
export const persistedSessionFromStored = (session: StoredSession): PersistedSession =>
  PersistedSession.make({
    version: 2,
    sessionId: session.sessionId,
    context: session.context,
    chat: chatHistoryFromMessages(session.messages),
    compactions: session.compactions,
  });

/** Migrate the former custom transcript representation into Effect AI Prompt messages. */
export const migrateLegacyStoredSession = (legacy: LegacyStoredSession): StoredSession => ({
  sessionId: legacy.sessionId,
  context: legacy.context,
  messages: legacy.messages.map((message) => ({
    id: message.id,
    createdAt: message.createdAt,
    message: promptMessageFromLegacy(message),
  })),
  compactions: legacy.compactions,
});

/** Narrow persistence capability required by the agent application service. */
export interface SessionStoreOperations {
  /** Load an existing record or create one with the supplied initial value. */
  readonly getOrCreate: (
    sessionId: SessionId,
    initial: StoredSession,
  ) => Effect.Effect<StoredSession, AgentPersistenceError>;
  /** Replace the durable record for its session. */
  readonly save: (session: StoredSession) => Effect.Effect<void, AgentPersistenceError>;
}

/** Effect Context service implemented by Durable Object storage in production. */
export class SessionStore extends Context.Service<SessionStore, SessionStoreOperations>()(
  "@alchemy-agent/SessionStore",
) {}
