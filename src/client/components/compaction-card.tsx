import { useAtomSet } from "@effect/atom-react";
import { compactSessionAtom } from "../agent-client.ts";
import type { SessionId, SessionSnapshot } from "../../shared/agent-protocol.ts";

/** Display model-context statistics and trigger non-destructive compaction. */
export function CompactionCard({
  sessionId,
  snapshot,
}: Readonly<{ sessionId: SessionId; snapshot: SessionSnapshot | undefined }>) {
  const compact = useAtomSet(compactSessionAtom);
  const stats = snapshot?.stats;
  return (
    <section className="control-card compaction-card">
      <p className="kicker">CHAT COMPACTION</p>
      <h2>Model context</h2>
      <dl className="metrics">
        <div>
          <dt>Raw messages</dt>
          <dd>{stats?.rawMessageCount ?? "—"}</dd>
        </div>
        <div>
          <dt>Model messages</dt>
          <dd>{stats?.modelMessageCount ?? "—"}</dd>
        </div>
        <div>
          <dt>Estimated tokens</dt>
          <dd>{stats?.estimatedModelTokens ?? "—"}</dd>
        </div>
        <div>
          <dt>Overlays</dt>
          <dd>{stats?.compactionCount ?? "—"}</dd>
        </div>
      </dl>
      <p>The transcript stays intact. Only the model-visible projection shrinks.</p>
      <button
        type="button"
        className="button button-outline"
        onClick={() =>
          compact({
            payload: { sessionId },
            reactivityKeys: [`session:${sessionId}`],
          })
        }
      >
        Compact eligible history
      </button>
    </section>
  );
}
