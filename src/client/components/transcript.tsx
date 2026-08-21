import type { SessionSnapshot } from "../../shared/agent-protocol.ts";
import { TranscriptRow } from "./transcript-row.tsx";

/** Render durable transcript history or an appropriate empty state. */
export function Transcript({ snapshot }: Readonly<{ snapshot: SessionSnapshot | undefined }>) {
  if (snapshot === undefined) {
    return <div className="empty-state">Opening the typed session…</div>;
  }
  if (snapshot.messages.length === 0) {
    return (
      <div className="empty-state">
        <strong>No messages yet.</strong>
        <span>Ask where the agent runs to exercise its typed tool.</span>
      </div>
    );
  }
  return (
    <ol className="transcript">
      {snapshot.messages.map((message) => (
        <TranscriptRow key={message.id} message={message} />
      ))}
    </ol>
  );
}
