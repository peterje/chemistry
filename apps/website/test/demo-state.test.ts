import { expect, test } from "bun:test";
import { nextCount, phaseLabel } from "../src/client/demo-state.ts";

test("nextCount advances the demo counter", () => {
  expect(nextCount(0)).toBe(1);
  expect(nextCount(3)).toBe(4);
});

test("phaseLabel covers every demo phase", () => {
  expect(phaseLabel("idle")).toBe("Idle");
  expect(phaseLabel("ready")).toBe("Ready");
});
