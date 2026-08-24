import { useState } from "react";
import type { RuntimeSocketSnapshot } from "@chemistry/client-runtime/resumable-agent-socket";
import { chatMessages, type SessionSnapshot } from "@chemistry/contracts/agent-protocol";
import {
  HeldTurn,
  LiveTurn,
  liveStartedUserId,
  nextHeldLiveTurns,
  type HeldLiveTurn,
} from "./live-turn.tsx";
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
  const [held, setHeld] = useState<ReadonlyArray<HeldLiveTurn>>([]);
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
  const messages = snapshot === undefined ? [] : chatMessages(snapshot.chat);
  const snapshotUserIds = new Set<string>();
  for (const message of messages) {
    if (message.message.role === "user") snapshotUserIds.add(message.id);
  }
  const nextHeld = nextHeldLiveTurns(held, runtime, snapshotUserIds);
  if (nextHeld !== held) setHeld(nextHeld);
  if (snapshot === undefined && runtime.recentEvents.length === 0 && nextHeld.length === 0) {
    return (
      <div className="transcript-shell transcript-loading" aria-label="Loading conversation">
        <span />
        <span />
        <span />
      </div>
    );
  }
  const liveUserId = liveStartedUserId(runtime);
  const persistedLiveUser =
    liveUserId !== undefined && messages.some((message) => message.id === liveUserId);
  const liveActive =
    runtime.status === "recovering" ||
    runtime.checkpoint === "admitted" ||
    runtime.checkpoint === "preparing" ||
    runtime.checkpoint === "streaming" ||
    runtime.checkpoint === "partial-persisted";
  const showLiveTurn =
    liveActive ||
    runtime.status === "failed" ||
    (runtime.status === "completed" && liveUserId !== undefined && !persistedLiveUser);
  const retainedTurns = nextHeld.filter((turn) => turn.userId !== liveUserId);
  if (messages.length === 0 && runtime.recentEvents.length === 0 && retainedTurns.length === 0) {
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
      {retainedTurns.map((turn) => (
        <HeldTurn key={turn.userId} turn={turn} />
      ))}
      {showLiveTurn ? <LiveTurn runtime={runtime} omitUser={persistedLiveUser} /> : null}
    </div>
  );
}
