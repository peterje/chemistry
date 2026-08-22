import type { RuntimeSocketSnapshot } from "@chemistry/client-runtime/resumable-agent-socket";
import type { AgentStreamEvent } from "@chemistry/contracts/agent-protocol";
import { MarkdownMessage } from "./markdown-message.tsx";

/** Render replay-safe in-flight user, assistant, and tool events within the transcript. */
export function LiveTurn({ runtime }: Readonly<{ runtime: RuntimeSocketSnapshot }>) {
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
    }
  }
  let prompt = "";
  if (started !== undefined) {
    for (const part of started.userMessage.parts) {
      if (part._tag === "Text") prompt += part.text;
    }
  }
  const active =
    runtime.status === "recovering" ||
    runtime.checkpoint === "admitted" ||
    runtime.checkpoint === "preparing" ||
    runtime.checkpoint === "streaming" ||
    runtime.checkpoint === "partial-persisted";

  if (!active && runtime.status !== "failed") return null;

  return (
    <div className="live-turn" aria-live="polite">
      {started !== undefined && (
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
