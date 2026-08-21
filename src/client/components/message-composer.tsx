import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { useState } from "react";
import { AgentRpcProtocol, makeAgentRpcClient } from "../agent-client.ts";
import type { AgentStreamEvent, SessionId } from "../../shared/agent-protocol.ts";

/** Send a message over a scoped Effect RPC stream and display live tool events. */
export function MessageComposer({
  sessionId,
  refreshSnapshot,
}: Readonly<{ sessionId: SessionId; refreshSnapshot: () => void }>) {
  const [prompt, setPrompt] = useState("");
  const [events, setEvents] = useState<ReadonlyArray<AgentStreamEvent>>([]);
  const [sending, setSending] = useState(false);
  const [failure, setFailure] = useState<string | undefined>();

  const submit = (event: React.SubmitEvent) => {
    event.preventDefault();
    const message = prompt.trim();
    if (message.length === 0 || sending) return;
    setPrompt("");
    setEvents([]);
    setFailure(undefined);
    setSending(true);

    const program = Effect.gen(function* () {
      const client = yield* makeAgentRpcClient;
      yield* client
        .sendMessage({ sessionId, prompt: message })
        .pipe(
          Stream.runForEach((streamEvent) =>
            Effect.sync(() => setEvents((current) => [...current, streamEvent])),
          ),
        );
    }).pipe(
      Effect.catch((error) => Effect.sync(() => setFailure(String(error)))),
      Effect.ensuring(
        Effect.sync(() => {
          setSending(false);
          refreshSnapshot();
        }),
      ),
      Effect.scoped,
      Effect.provide(AgentRpcProtocol),
    );
    Effect.runFork(program);
  };

  const toolEvents: Array<Extract<AgentStreamEvent, { readonly _tag: "ToolCall" | "ToolResult" }>> =
    [];
  let streamedText = "";
  for (const streamEvent of events) {
    if (streamEvent._tag === "TextDelta") streamedText += streamEvent.delta;
    if (streamEvent._tag === "ToolCall" || streamEvent._tag === "ToolResult") {
      toolEvents.push(streamEvent);
    }
  }

  return (
    <div className="composer-wrap">
      {streamedText.length > 0 && (
        <div className="live-assistant" aria-live="polite">
          <span>ASSISTANT · LIVE</span>
          <p>{streamedText}</p>
        </div>
      )}
      {toolEvents.length > 0 && (
        <div className="live-events" aria-live="polite">
          {toolEvents.map((event, index) => (
            <span key={`${event._tag}-${index}`}>
              {event._tag === "ToolCall" ? "Calling" : "Resolved"} {event.name}
            </span>
          ))}
        </div>
      )}
      {failure !== undefined && <p className="error-text">{failure}</p>}
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
        <button className="button button-accent" type="submit" disabled={sending}>
          {sending ? "Thinking…" : "Send via Effect Stream"}
        </button>
      </form>
    </div>
  );
}
