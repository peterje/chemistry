import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";
import type {
  AgentRpcError,
  AgentStreamEvent,
  MessageId,
  OperationId,
  SessionId,
  StreamId,
  SubmissionId,
} from "../shared/agent-protocol.ts";

/** Input for one fresh or transcript-continuation model turn. */
export interface TurnExecutionInput {
  /** Durable session that owns the turn. */
  readonly sessionId: SessionId;
  /** Durable operation identity. */
  readonly operationId: OperationId;
  /** Client idempotency identity. */
  readonly submissionId: SubmissionId;
  /** Durable stream identity. */
  readonly streamId: StreamId;
  /** Original user prompt. */
  readonly prompt: string;
  /** Deterministic transcript identity for the user message. */
  readonly userMessageId: MessageId;
  /** Deterministic transcript identity for recovered assistant content. */
  readonly assistantMessageId: MessageId;
  /** Whether this call starts fresh or continues a persisted partial. */
  readonly mode: "fresh" | "continue";
}

/** Partial assistant content reconstructed before transcript continuation. */
export interface PersistPartialInput {
  /** Durable session that owns the transcript. */
  readonly sessionId: SessionId;
  /** Deterministic assistant message identity. */
  readonly assistantMessageId: MessageId;
  /** Safely reconstructed text emitted before interruption. */
  readonly text: string;
}

/** Model-facing turn capability required by durable execution. */
export interface TurnExecutorOperations {
  /** Execute one fresh or continuation model turn as typed domain events. */
  readonly execute: (input: TurnExecutionInput) => Stream.Stream<AgentStreamEvent, AgentRpcError>;
  /** Idempotently persist a reconstructed partial assistant message. */
  readonly persistPartial: (input: PersistPartialInput) => Effect.Effect<void, AgentRpcError>;
  /** Determine whether deterministic assistant content is already durable. */
  readonly hasAssistantMessage: (
    sessionId: SessionId,
    assistantMessageId: MessageId,
  ) => Effect.Effect<boolean, AgentRpcError>;
}

/** Effect service adapting the existing agent conversation module to durable turns. */
export class TurnExecutor extends Context.Service<TurnExecutor, TurnExecutorOperations>()(
  "@chemistry/TurnExecutor",
) {}
