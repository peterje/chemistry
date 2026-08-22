import { useEffect, useState } from "react";
import type { ResumableAgentSocket, RuntimeSocketSnapshot } from "../resumable-agent-socket.ts";

/** Submit turns over the resumable WebSocket and render replay-safe live events. */
export function MessageComposer({
  socket,
  runtime,
  refreshSnapshot,
}: Readonly<{
  socket: ResumableAgentSocket;
  runtime: RuntimeSocketSnapshot;
  refreshSnapshot: () => void;
}>) {
  const [prompt, setPrompt] = useState("");

  useEffect(() => {
    if (runtime.status === "completed" || runtime.status === "failed") {
      refreshSnapshot();
    }
  }, [refreshSnapshot, runtime.status, runtime.streamId]);

  const submit = (event: React.SubmitEvent) => {
    event.preventDefault();
    const message = prompt.trim();
    if (message.length === 0) return;
    if (socket.submit(message) !== undefined) setPrompt("");
  };

  const events = runtime.recentEvents.map((event) => event.event);
  const toolEvents: Array<
    Extract<(typeof events)[number], { readonly _tag: "ToolCall" | "ToolResult" }>
  > = [];
  let streamedText = "";
  for (const streamEvent of events) {
    if (streamEvent._tag === "TextDelta") streamedText += streamEvent.delta;
    if (streamEvent._tag === "ToolCall" || streamEvent._tag === "ToolResult") {
      toolEvents.push(streamEvent);
    }
  }

  const turnInFlight =
    runtime.status === "recovering" ||
    runtime.checkpoint === "admitted" ||
    runtime.checkpoint === "preparing" ||
    runtime.checkpoint === "streaming" ||
    runtime.checkpoint === "partial-persisted";
  const unavailable =
    runtime.status === "connecting" ||
    runtime.status === "replaying" ||
    runtime.status === "interrupted" ||
    runtime.status === "disconnected";

  return (
    <div className="composer-wrap">
      {streamedText.length > 0 && (
        <div className="live-assistant" aria-live="polite">
          <span>
            ASSISTANT · {runtime.status === "replaying" ? "REPLAY" : "LIVE"} · #
            {runtime.lastSequence}
          </span>
          <p>{streamedText}</p>
        </div>
      )}
      {toolEvents.length > 0 && (
        <div className="live-events" aria-live="polite">
          {toolEvents.map((event, index) => (
            <span key={`${event._tag}-${event.callId}-${index}`}>
              {event._tag === "ToolCall" ? "Calling" : "Resolved"} {event.name}
            </span>
          ))}
        </div>
      )}
      {runtime.error !== null && <p className="error-text">{runtime.error}</p>}
      <form className="composer" onSubmit={submit}>
        <label htmlFor="message">Message the agent</label>
        <textarea
          id="message"
          value={prompt}
          onChange={(event) => setPrompt(event.target.value)}
          placeholder="Ask: Where does this agent run?"
          rows={3}
          maxLength={32_000}
        />
        <button
          className="button button-accent"
          type="submit"
          disabled={turnInFlight || unavailable}
        >
          {turnInFlight ? "Thinking…" : "Send via resumable WebSocket"}
        </button>
      </form>
    </div>
  );
}
