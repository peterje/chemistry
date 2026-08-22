import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  BootId,
  ConnectionId,
  OperationId,
  RuntimeClientFrame,
  RuntimeServerFrame,
  RuntimeSnapshot,
  SessionId,
  StreamId,
  SubmissionId,
} from "../src/shared/agent-protocol.ts";
import {
  MAX_RUNTIME_FRAME_BYTES,
  decodeRuntimeClientFrame,
  decodeRuntimeServerFrame,
  encodeRuntimeClientFrame,
  encodeRuntimeServerFrame,
} from "../src/shared/runtime-protocol.ts";

describe("shared runtime WebSocket protocol", () => {
  test("round-trips typed resume frames through the shared schemas", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const clientFrame = RuntimeClientFrame.cases.ResumeAck.make({
          probeId: "probe-1",
          streamId: StreamId.make("stream-1"),
          afterSequence: 7,
        });
        const clientValue = yield* Schema.encodeUnknownEffect(RuntimeClientFrame)(clientFrame);
        const decodedClient = yield* decodeRuntimeClientFrame(JSON.stringify(clientValue));
        expect(decodedClient).toEqual(clientFrame);

        const serverFrame = RuntimeServerFrame.cases.ResumeProbe.make({
          probeId: "probe-1",
          connectionId: ConnectionId.make("connection-1"),
          sessionId: SessionId.make("session-1"),
          activeStreamId: StreamId.make("stream-1"),
          latestSequence: 7,
          runtime: RuntimeSnapshot.make({
            bootId: BootId.make("boot-1"),
            activeOperation: null,
            queueDepth: 0,
            retainedStreamCount: 1,
            recoveryAttempt: 0,
            lastTerminalReason: null,
          }),
        });
        const encodedServer = yield* encodeRuntimeServerFrame(serverFrame);
        const decodedServer = yield* decodeRuntimeServerFrame(encodedServer);
        expect(decodedServer).toEqual(serverFrame);
      }),
    ));

  test("rejects malformed, stale-shaped, and oversized frames as typed errors", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const invalidJson = yield* decodeRuntimeClientFrame("{").pipe(Effect.flip);
        expect(invalidJson._tag).toBe("AgentProtocolError");
        expect(invalidJson.code).toBe("invalid-json");

        const malformedResume = yield* decodeRuntimeClientFrame(
          JSON.stringify({ _tag: "ResumeAck", probeId: "probe-1", afterSequence: "seven" }),
        ).pipe(Effect.flip);
        expect(malformedResume.code).toBe("invalid-frame");

        const oversized = yield* decodeRuntimeClientFrame(
          "x".repeat(MAX_RUNTIME_FRAME_BYTES + 1),
        ).pipe(Effect.flip);
        expect(oversized.code).toBe("frame-too-large");
      }),
    ));

  test("keeps turn submissions and liveness frames schema-bounded", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const submission = RuntimeClientFrame.cases.SubmitTurn.make({
          submissionId: SubmissionId.make("submission-1"),
          prompt: "hello",
        });
        const encoded = yield* Schema.encodeUnknownEffect(RuntimeClientFrame)(submission);
        expect(yield* decodeRuntimeClientFrame(JSON.stringify(encoded))).toEqual(submission);

        const keepAlive = RuntimeClientFrame.cases.KeepAlive.make({});
        expect(yield* decodeRuntimeClientFrame(yield* encodeRuntimeClientFrame(keepAlive))).toEqual(
          keepAlive,
        );
        const keepAliveAck = RuntimeServerFrame.cases.KeepAliveAck.make({});
        expect(
          yield* decodeRuntimeServerFrame(yield* encodeRuntimeServerFrame(keepAliveAck)),
        ).toEqual(keepAliveAck);

        const terminal = RuntimeServerFrame.cases.StreamTerminal.make({
          streamId: StreamId.make("stream-1"),
          operationId: OperationId.make("operation-1"),
          status: "completed",
          sequence: 3,
          generation: 1,
          attempt: 0,
          recoveryWork: 0,
          reason: null,
        });
        expect(yield* decodeRuntimeServerFrame(yield* encodeRuntimeServerFrame(terminal))).toEqual(
          terminal,
        );
      }),
    ));
});
