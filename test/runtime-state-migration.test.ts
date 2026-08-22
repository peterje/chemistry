import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { BootId, SessionId } from "../src/shared/agent-protocol.ts";
import { RuntimeState, initialRuntimeState } from "../src/server/runtime-state.ts";
import { RuntimeStore } from "../src/server/runtime-store.ts";
import { RuntimeStoreTest, RuntimeStoreTestLayer } from "./support/runtime-store-test-layer.ts";

describe("versioned runtime migration", () => {
  test("creates a separate empty runtime record when an existing session has none", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const store = yield* RuntimeStore;
        const controls = yield* RuntimeStoreTest;
        const sessionId = SessionId.make("legacy-session");
        const initial = initialRuntimeState(sessionId, BootId.make("migration-boot"), 10);
        expect(yield* store.load(sessionId, initial)).toEqual(initial);
        expect((yield* controls.inspect(sessionId))._tag).toBe("Some");
      }).pipe(Effect.provide(RuntimeStoreTestLayer)),
    ));

  test("rejects unknown runtime versions instead of silently resetting them", () => {
    const decoded = Schema.decodeUnknownResult(RuntimeState)({
      version: 2,
      sessionId: "legacy-session",
      bootId: "migration-boot",
      generation: 0,
      operations: [],
      queue: [],
      activeOperationId: null,
      streams: [],
      recovery: null,
      alarm: null,
      lastTerminalReason: null,
      lastWakeAt: 10,
    });
    expect(Result.isFailure(decoded)).toBe(true);
  });
});
