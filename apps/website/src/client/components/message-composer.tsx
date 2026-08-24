import { useEffect, useState } from "react";
import type {
  ResumableAgentSocket,
  RuntimeSocketSnapshot,
} from "@chemistry/client-runtime/resumable-agent-socket";

/** Submit turns over the resumable WebSocket from the sticky chat composer. */
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
    if (
      runtime.status === "completed" ||
      runtime.status === "failed" ||
      runtime.checkpoint === "streaming"
    ) {
      refreshSnapshot();
    }
  }, [refreshSnapshot, runtime.status, runtime.streamId, runtime.checkpoint]);

  const submit = (event: React.SubmitEvent) => {
    event.preventDefault();
    const message = prompt.trim();
    if (message.length === 0) return;
    if (socket.submit(message) !== undefined) setPrompt("");
  };

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
  const canSend = prompt.trim().length > 0 && !turnInFlight && !unavailable;

  return (
    <div className="composer-wrap">
      {runtime.error !== null && <p className="composer-error">{runtime.error}</p>}
      <form className="composer" onSubmit={submit}>
        <label className="sr-only" htmlFor="message">
          Message Chemistry
        </label>
        <textarea
          id="message"
          value={prompt}
          onChange={(event) => setPrompt(event.target.value)}
          onKeyDown={(event) => {
            if (event.key !== "Enter" || event.shiftKey || event.nativeEvent.isComposing) return;
            event.preventDefault();
            event.currentTarget.form?.requestSubmit();
          }}
          placeholder="Message Chemistry"
          rows={2}
          maxLength={32_000}
        />
        <button
          type="submit"
          className="composer-send"
          disabled={!canSend}
          aria-label="Send message"
        >
          <span aria-hidden="true">↑</span>
        </button>
      </form>
      <p className="composer-note">Chemistry can make mistakes. Check important information.</p>
    </div>
  );
}
