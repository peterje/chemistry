import type { RuntimeSocketSnapshot } from "@chemistry/client-runtime/resumable-agent-socket";
import type { AgentStreamEvent } from "@chemistry/contracts/agent-protocol";
import { MarkdownMessage } from "./markdown-message.tsx";

/** Completed live turn retained until the durable transcript snapshot includes it. */
export interface HeldLiveTurn {
  /** User message identity for the retained turn. */
  readonly userId: string;
  /** User prompt text captured from TurnStarted. */
  readonly prompt: string;
  /** Assistant text captured from TextDelta events. */
  readonly text: string;
  /** Durable terminal status of the retained turn. */
  readonly status: "completed" | "failed";
  /** Last durable terminal reason, if the server reported one. */
  readonly terminalReason: string | null;
  /** Browser-visible transport or protocol diagnostic. */
  readonly error: string | null;
}

/** User message identity for the in-flight turn, if a TurnStarted event has arrived. */
export const liveStartedUserId = (runtime: RuntimeSocketSnapshot): string | undefined => {
  for (const durableEvent of runtime.recentEvents) {
    if (durableEvent.event._tag === "TurnStarted") return durableEvent.event.userMessage.id;
  }
  return undefined;
};

const promptFromStarted = (
  started: Extract<AgentStreamEvent, { readonly _tag: "TurnStarted" }>,
): string => {
  let prompt = "";
  for (const part of started.userMessage.parts) {
    if (part._tag === "Text") prompt += part.text;
  }
  return prompt;
};

/** Snapshot a completed or failed live turn so follow-up submits cannot erase it. */
export const captureHeldLiveTurn = (runtime: RuntimeSocketSnapshot): HeldLiveTurn | undefined => {
  if (runtime.status !== "completed" && runtime.status !== "failed") return undefined;
  const userId = liveStartedUserId(runtime);
  if (userId === undefined) return undefined;
  let prompt = "";
  let text = "";
  for (const durableEvent of runtime.recentEvents) {
    const event = durableEvent.event;
    switch (event._tag) {
      case "TurnStarted":
        prompt = promptFromStarted(event);
        break;
      case "TextDelta":
        text += event.delta;
        break;
      case "ToolCall":
      case "ToolResult":
      case "CompactionCompleted":
      case "TurnCompleted":
        break;
      default: {
        const _exhaustive: never = event;
        void _exhaustive;
      }
    }
  }
  return {
    userId,
    prompt,
    text,
    status: runtime.status,
    terminalReason: runtime.terminalReason,
    error: runtime.error,
  };
};

const sameHeldLiveTurn = (left: HeldLiveTurn, right: HeldLiveTurn): boolean =>
  left.userId === right.userId &&
  left.prompt === right.prompt &&
  left.text === right.text &&
  left.status === right.status &&
  left.terminalReason === right.terminalReason &&
  left.error === right.error;

/** Merge a newly completed live turn and drop turns the durable snapshot already has. */
export const nextHeldLiveTurns = (
  previous: ReadonlyArray<HeldLiveTurn>,
  runtime: RuntimeSocketSnapshot,
  snapshotUserIds: ReadonlySet<string>,
): ReadonlyArray<HeldLiveTurn> => {
  const captured = captureHeldLiveTurn(runtime);
  const merged =
    captured === undefined
      ? previous
      : previous.some((turn) => turn.userId === captured.userId)
        ? previous.map((turn) => (turn.userId === captured.userId ? captured : turn))
        : [...previous, captured];
  const filtered = merged.filter((turn) => !snapshotUserIds.has(turn.userId));
  if (
    filtered.length === previous.length &&
    filtered.every((turn, index) => {
      const prior = previous[index];
      return prior !== undefined && sameHeldLiveTurn(turn, prior);
    })
  ) {
    return previous;
  }
  return filtered;
};

/** Render a completed live turn that is waiting for the transcript snapshot. */
export function HeldTurn({ turn }: Readonly<{ turn: HeldLiveTurn }>) {
  return (
    <div className="live-turn" aria-live="polite">
      <div className="message message-user live-message">
        <div className="message-body">
          <p className="user-text">{turn.prompt}</p>
        </div>
      </div>
      <div className="message message-assistant live-message">
        <span className="assistant-avatar" aria-label="Chemistry">
          C
        </span>
        <div className="message-body">
          {turn.text.length > 0 ? <MarkdownMessage>{turn.text}</MarkdownMessage> : null}
          {turn.status === "failed" && (
            <p className="turn-error">{turn.terminalReason ?? turn.error ?? "The turn failed."}</p>
          )}
        </div>
      </div>
    </div>
  );
}

/** Render replay-safe in-flight user, assistant, and tool events within the transcript. */
export function LiveTurn({
  runtime,
  omitUser = false,
}: Readonly<{ runtime: RuntimeSocketSnapshot; omitUser?: boolean }>) {
  let started: Extract<AgentStreamEvent, { readonly _tag: "TurnStarted" }> | undefined;
  let text = "";
  const tools: Array<Extract<AgentStreamEvent, { readonly _tag: "ToolCall" | "ToolResult" }>> = [];
  for (const durableEvent of runtime.recentEvents) {
    const event = durableEvent.event;
    switch (event._tag) {
      case "TurnStarted":
        started = event;
        break;
      case "TextDelta":
        text += event.delta;
        break;
      case "ToolCall":
      case "ToolResult":
        tools.push(event);
        break;
      case "CompactionCompleted":
      case "TurnCompleted":
        break;
      default: {
        const _exhaustive: never = event;
        void _exhaustive;
      }
    }
  }
  const prompt = started === undefined ? "" : promptFromStarted(started);
  const active =
    runtime.status === "recovering" ||
    runtime.checkpoint === "admitted" ||
    runtime.checkpoint === "preparing" ||
    runtime.checkpoint === "streaming" ||
    runtime.checkpoint === "partial-persisted";

  if (!active && runtime.status !== "failed" && runtime.status !== "completed") return null;

  return (
    <div className="live-turn" aria-live="polite">
      {started !== undefined && !omitUser && (
        <div className="message message-user live-message">
          <div className="message-body">
            <p className="user-text">{prompt}</p>
          </div>
        </div>
      )}
      <div className="message message-assistant live-message">
        <span className="assistant-avatar" aria-label="Chemistry">
          C
        </span>
        <div className="message-body">
          {tools.map((event) => (
            <div className="tool-progress" key={`${event.callId}-${event._tag}`}>
              <span className="tool-spinner" aria-hidden="true" />
              {event._tag === "ToolCall" ? "Using" : "Used"} {event.name}
            </div>
          ))}
          {text.length > 0 ? (
            <MarkdownMessage>{text}</MarkdownMessage>
          ) : active ? (
            <div className="thinking-indicator" aria-label="Assistant is thinking">
              <span />
              <span />
              <span />
            </div>
          ) : null}
          {runtime.status === "recovering" && (
            <p className="turn-notice">Reconnecting to the durable response…</p>
          )}
          {runtime.status === "failed" && (
            <p className="turn-error">
              {runtime.terminalReason ?? runtime.error ?? "The turn failed."}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
