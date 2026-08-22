import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import {
  BootId,
  MessageId,
  OperationId,
  SessionId,
  StreamId,
  SubmissionId,
} from "../src/shared/agent-protocol.ts";
import {
  LegacyRuntimeStateV1,
  RuntimeState,
  initialRuntimeState,
  migrateRuntimeStateV1,
} from "../src/server/runtime-state.ts";
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

  test("migrates v1 operations into typed input, request snapshot, and phase ledger", () => {
    const sessionId = SessionId.make("runtime-v1-session");
    const operationId = OperationId.make("runtime-v1-operation");
    const streamId = StreamId.make("runtime-v1-stream");
    const legacy = LegacyRuntimeStateV1.make({
      version: 1,
      sessionId,
      bootId: BootId.make("runtime-v1-boot"),
      generation: 1,
      operations: [
        {
          operationId,
          submissionId: SubmissionId.make("runtime-v1-submission"),
          streamId,
          prompt: "preserve this input",
          userMessageId: MessageId.make("runtime-v1-user"),
          assistantMessageId: MessageId.make("runtime-v1-assistant"),
          status: "queued",
          checkpoint: "admitted",
          generation: 0,
          attempt: 0,
          progress: 0,
          recoveryWork: 0,
          ownerBootId: null,
          leaseExpiresAt: null,
          createdAt: 1,
          updatedAt: 1,
          terminalReason: null,
        },
      ],
      queue: [operationId],
      activeOperationId: null,
      streams: [
        {
          streamId,
          operationId,
          status: "open",
          latestSequence: -1,
          encodedBytes: 0,
          events: [],
          createdAt: 1,
          updatedAt: 1,
          expiresAt: null,
          terminalReason: null,
        },
      ],
      recovery: null,
      alarm: null,
      lastTerminalReason: null,
      lastWakeAt: 1,
    });
    const migrated = migrateRuntimeStateV1(legacy);
    expect(migrated.version).toBe(2);
    expect(migrated.operations[0]?.kind).toBe("agent-turn");
    expect(migrated.operations[0]?.input.prompt).toBe("preserve this input");
    expect(migrated.operations[0]?.effectLedger).toHaveLength(2);
    expect(migrated.queue).toEqual([operationId]);
  });

  test("rejects unknown runtime versions instead of silently resetting them", () => {
    const decoded = Schema.decodeUnknownResult(RuntimeState)({
      version: 3,
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
