/** Demo readiness phases used to keep switch handling exhaustive. */
export type DemoPhase = "idle" | "ready";

/** Human-readable label for one demo phase. */
export const phaseLabel = (phase: DemoPhase): string => {
  switch (phase) {
    case "idle":
      return "Idle";
    case "ready":
      return "Ready";
    default: {
      const _exhaustive: never = phase;
      return _exhaustive;
    }
  }
};

/** Next demo counter value. */
export const nextCount = (count: number): number => count + 1;
