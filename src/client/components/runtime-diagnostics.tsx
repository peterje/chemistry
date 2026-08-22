import type { RuntimeSocketSnapshot } from "../resumable-agent-socket.ts";

/** Render durable stream, checkpoint, and recovery evidence from the live runtime. */
export function RuntimeDiagnostics({ runtime }: Readonly<{ runtime: RuntimeSocketSnapshot }>) {
  return (
    <section className="runtime-card" aria-label="Durable runtime diagnostics">
      <div className="card-heading">
        <div>
          <p className="kicker">RUNTIME + RECOVERY</p>
          <h2>Durable execution</h2>
        </div>
        <strong data-runtime-status={runtime.status}>{runtime.status}</strong>
      </div>
      <dl className="stats-grid">
        <div>
          <dt>Stream</dt>
          <dd>{runtime.streamId ?? "none"}</dd>
        </div>
        <div>
          <dt>Sequence</dt>
          <dd>{runtime.lastSequence}</dd>
        </div>
        <div>
          <dt>Checkpoint</dt>
          <dd>{runtime.checkpoint ?? "idle"}</dd>
        </div>
        <div>
          <dt>Recovery</dt>
          <dd>attempt {runtime.recoveryAttempt}</dd>
        </div>
      </dl>
      {runtime.terminalReason !== null && (
        <p className="runtime-reason">Terminal: {runtime.terminalReason}</p>
      )}
      {runtime.error !== null && <p className="error-text">{runtime.error}</p>}
    </section>
  );
}
