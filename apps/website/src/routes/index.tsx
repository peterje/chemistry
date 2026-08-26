import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import { nextCount, phaseLabel, type DemoPhase } from "../client/demo-state.ts";

/** Landing page for the starter demo. */
export const Route = createFileRoute("/")({
  ssr: false,
  component: HomePage,
});

function HomePage() {
  const [count, setCount] = useState(0);
  const [phase, setPhase] = useState<DemoPhase>("idle");

  return (
    <main className="demo-app">
      <p className="eyebrow">Alchemy starter</p>
      <h1>Starter</h1>
      <p className="lede">
        A small Vite + React app with the same verification toolchain as Chemistry: Alchemy, oxlint
        anti-slop, oxfmt, lefthook, Playwright, and React Doctor.
      </p>
      <p className="phase" data-phase={phase}>
        {phaseLabel(phase)}
      </p>
      <div className="demo-actions">
        <button
          type="button"
          className="primary"
          onClick={() => {
            setCount(nextCount(count));
            setPhase("ready");
          }}
        >
          Increment
        </button>
        <p className="count" aria-live="polite">
          Count {count}
        </p>
      </div>
    </main>
  );
}
