import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  AgentContext,
  AgentPersistenceError,
  Compaction,
  SessionId,
  TranscriptMessage,
} from "../shared/agent-protocol.ts";

/** Persisted session state from which model context and UI snapshots are derived. */
export const StoredSession = Schema.Struct({
  sessionId: SessionId,
  context: AgentContext,
  messages: Schema.Array(TranscriptMessage),
  compactions: Schema.Array(Compaction),
}).annotate({ identifier: "StoredSession" });

/** A decoded durable session record. */
export interface StoredSession extends Schema.Schema.Type<typeof StoredSession> {}

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
