import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { createFileRoute } from "@tanstack/react-router";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { useMemo, useState } from "react";
import { CompactionCard } from "../client/components/compaction-card.tsx";
import { ContextEditor } from "../client/components/context-editor.tsx";
import { MessageComposer } from "../client/components/message-composer.tsx";
import { StatusPill } from "../client/components/status-pill.tsx";
import { Transcript } from "../client/components/transcript.tsx";
import { sessionSnapshotAtom } from "../client/agent-client.ts";
import { SessionId } from "../shared/agent-protocol.ts";

/** Root route for the complete Effect-native agent demonstration. */
export const Route = createFileRoute("/")({
  ssr: false,
  component: AgentDemo,
});

const defaultSessionId = SessionId.make("demo-session");
const decodeSessionId = Schema.decodeUnknownResult(SessionId);

function AgentDemo() {
  const [sessionId, setSessionId] = useState(defaultSessionId);
  const [sessionDraft, setSessionDraft] = useState<string>(sessionId);
  const [sessionError, setSessionError] = useState<string | undefined>();
  const snapshotAtom = useMemo(() => sessionSnapshotAtom(sessionId), [sessionId]);
  const snapshotResult = useAtomValue(snapshotAtom);
  const refreshSnapshot = useAtomRefresh(snapshotAtom);
  const snapshot = AsyncResult.isSuccess(snapshotResult) ? snapshotResult.value : undefined;

  const selectSession = (event: React.SubmitEvent) => {
    event.preventDefault();
    const decoded = decodeSessionId(sessionDraft.trim());
    if (Result.isFailure(decoded)) {
      setSessionError(
        "Use 1–64 letters, numbers, underscores, or hyphens; start with a letter or number.",
      );
      return;
    }
    setSessionError(undefined);
    setSessionId(decoded.success);
  };

  return (
    <main className="app-shell">
      <header className="masthead">
        <div>
          <p className="eyebrow">ALCHEMY × EFFECT × CLOUDFLARE</p>
          <h1>
            Durable thought,
            <br />
            typed end to end.
          </h1>
        </div>
        <div className="architecture-note">
          <span>React</span>
          <i>→</i>
          <span>Effect RPC</span>
          <i>→</i>
          <span>Worker</span>
          <i>→</i>
          <span>Durable Object</span>
          <i>→</i>
          <span>Workers AI</span>
        </div>
      </header>

      <section className="session-bar" aria-label="Session selection">
        <form onSubmit={selectSession}>
          <label htmlFor="session-id">Durable session</label>
          <input
            id="session-id"
            value={sessionDraft}
            onChange={(event) => setSessionDraft(event.target.value)}
            pattern="[a-zA-Z0-9][a-zA-Z0-9_-]*"
            maxLength={64}
          />
          <button type="submit" className="button button-dark">
            Open
          </button>
        </form>
        <p>{sessionError ?? `One Durable Object: ${sessionId}`}</p>
      </section>

      <div className="workspace-grid">
        <section className="conversation-panel">
          <div className="section-heading">
            <div>
              <p className="kicker">MESSAGE SENDING + TOOLS</p>
              <h2>Conversation</h2>
            </div>
            <StatusPill result={snapshotResult} />
          </div>
          <Transcript snapshot={snapshot} />
          <MessageComposer sessionId={sessionId} refreshSnapshot={refreshSnapshot} />
        </section>

        <aside className="control-panel">
          {snapshot !== undefined && (
            <ContextEditor
              key={`${sessionId}:${snapshot.context.systemPrompt}:${snapshot.context.memory}`}
              sessionId={sessionId}
              initialContext={snapshot.context}
            />
          )}
          <CompactionCard sessionId={sessionId} snapshot={snapshot} />
        </aside>
      </div>
    </main>
  );
}
