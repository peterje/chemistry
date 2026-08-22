import type { RuntimeSocketSnapshot } from "@chemistry/client-runtime/resumable-agent-socket";
import type { SessionSnapshot } from "@chemistry/contracts/agent-protocol";
import { LiveTurn } from "./live-turn.tsx";
import { TranscriptRow } from "./transcript-row.tsx";

/** Render durable transcript history plus the replay-safe in-flight turn. */
export function Transcript({
  snapshot,
  runtime,
  failed,
  onRetry,
}: Readonly<{
  snapshot: SessionSnapshot | undefined;
  runtime: RuntimeSocketSnapshot;
  failed: boolean;
  onRetry: () => void;
}>) {
  if (failed) {
    return (
      <div className="query-error" role="alert">
        <div className="empty-mark" aria-hidden="true">
          C
        </div>
        <h1>We couldn’t load this conversation.</h1>
        <p>Your durable messages are still safe. Check the connection and try again.</p>
        <button type="button" onClick={onRetry}>
          Try again
        </button>
      </div>
    );
  }
  if (snapshot === undefined && runtime.recentEvents.length === 0) {
    return (
      <div className="transcript-shell transcript-loading" aria-label="Loading conversation">
        <span />
        <span />
        <span />
      </div>
    );
  }
  const messages = snapshot?.messages ?? [];
  if (messages.length === 0 && runtime.recentEvents.length === 0) {
    return (
      <div className="empty-state">
        <div className="empty-mark" aria-hidden="true">
          C
        </div>
        <h1>What can I help with?</h1>
        <p>Ask a question, explore an idea, or put the durable runtime to work.</p>
      </div>
    );
  }
  return (
    <div className="transcript-shell">
      <ol className="transcript">
        {messages.map((message) => (
          <TranscriptRow key={message.id} message={message} />
        ))}
      </ol>
      <LiveTurn runtime={runtime} />
    </div>
  );
}
