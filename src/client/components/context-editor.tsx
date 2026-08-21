import { useAtomSet } from "@effect/atom-react";
import { useState } from "react";
import { updateContextAtom } from "../agent-client.ts";
import { AgentContext, type SessionId } from "../../shared/agent-protocol.ts";

/** Edit and persist system instructions plus the durable memory block. */
export function ContextEditor({
  sessionId,
  initialContext,
}: Readonly<{ sessionId: SessionId; initialContext: AgentContext }>) {
  const updateContext = useAtomSet(updateContextAtom);
  const [systemPrompt, setSystemPrompt] = useState(initialContext.systemPrompt);
  const [memory, setMemory] = useState(initialContext.memory);

  const save = (event: React.SubmitEvent) => {
    event.preventDefault();
    const context = AgentContext.make({
      systemPrompt: systemPrompt.trim() || "You are a concise assistant.",
      memory,
    });
    updateContext({
      payload: { sessionId, context },
      reactivityKeys: [`session:${sessionId}`],
    });
  };

  return (
    <section className="control-card">
      <p className="kicker">CONTEXT</p>
      <h2>Durable memory</h2>
      <p>Both blocks are injected into every Workers AI request.</p>
      <form onSubmit={save}>
        <label htmlFor="system-prompt">System instructions</label>
        <textarea
          id="system-prompt"
          rows={5}
          value={systemPrompt}
          onChange={(event) => setSystemPrompt(event.target.value)}
        />
        <label htmlFor="memory">Session memory</label>
        <textarea
          id="memory"
          rows={5}
          value={memory}
          onChange={(event) => setMemory(event.target.value)}
          placeholder="Preferences, decisions, facts…"
        />
        <button type="submit" className="button button-dark">
          Save context
        </button>
      </form>
    </section>
  );
}
