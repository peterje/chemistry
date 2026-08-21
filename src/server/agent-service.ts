import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";
import type {
  AgentContext,
  AgentRpcError,
  AgentStreamEvent,
  CompactionResult,
  SessionId,
  SessionSnapshot,
} from "../shared/agent-protocol.ts";

/** Runtime policy controlling bounded inference and compaction. */
export interface AgentConfiguration {
  /** Maximum inference/tool rounds in one user turn. */
  readonly maxToolSteps: number;
  /** Estimated model tokens that trigger automatic compaction. */
  readonly compactionThresholdTokens: number;
  /** Number of recent transcript messages retained at full fidelity. */
  readonly retainRecentMessages: number;
}

/** Default runtime policy used by the demonstration. */
export const defaultAgentConfiguration: AgentConfiguration = {
  maxToolSteps: 5,
  compactionThresholdTokens: 1_200,
  retainRecentMessages: 6,
};

/** Application operations implemented by one durable agent runtime. */
export interface AgentServiceShape {
  /** Load or initialize one session snapshot. */
  readonly getSession: (sessionId: SessionId) => Effect.Effect<SessionSnapshot, AgentRpcError>;
  /** Replace durable context and return the resulting snapshot. */
  readonly updateContext: (
    sessionId: SessionId,
    context: AgentContext,
  ) => Effect.Effect<SessionSnapshot, AgentRpcError>;
  /** Execute and stream one durable user turn. */
  readonly sendMessage: (
    sessionId: SessionId,
    prompt: string,
  ) => Stream.Stream<AgentStreamEvent, AgentRpcError>;
  /** Compact eligible history on demand. */
  readonly compactSession: (sessionId: SessionId) => Effect.Effect<CompactionResult, AgentRpcError>;
}

/** Effect Context service for the Think-inspired agent application module. */
export class AgentService extends Context.Service<AgentService, AgentServiceShape>()(
  "@alchemy-agent/AgentService",
) {}
