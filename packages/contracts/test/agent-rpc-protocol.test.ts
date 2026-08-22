import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import {
  AgentInferenceError,
  AgentRpcError,
  AgentRpcs,
  AgentStreamEvent,
  MessageId,
  SessionId,
  TranscriptMessage,
  TranscriptPart,
} from "@chemistry/contracts/agent-protocol";

const sendMessageRpc = AgentRpcs.requests.get("sendMessage");

describe("shared AgentRpcs schema and NDJSON serialization", () => {
  test("decodes branded payloads and rejects malformed session ids", () => {
    expect([...AgentRpcs.requests.keys()]).toEqual([
      "createChat",
      "listChats",
      "getSession",
      "getRuntime",
      "updateContext",
      "sendMessage",
      "compactSession",
    ]);
    expect(sendMessageRpc).toBeDefined();
    if (sendMessageRpc === undefined) return;

    const decodePayload = Schema.decodeUnknownResult(sendMessageRpc.payloadSchema);
    const valid = decodePayload({ sessionId: "session-1", prompt: "hello" });
    expect(Result.isSuccess(valid)).toBe(true);
    if (Result.isSuccess(valid)) {
      expect(valid.success).toEqual({
        sessionId: SessionId.make("session-1"),
        prompt: "hello",
      });
    }
    expect(
      Result.isFailure(decodePayload({ sessionId: "spaces are invalid", prompt: "hello" })),
    ).toBe(true);
  });

  test("round-trips stream events and typed errors through schemas and NDJSON", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const assistantMessage = TranscriptMessage.make({
          id: MessageId.make("message-1"),
          role: "assistant",
          parts: [TranscriptPart.cases.Text.make({ text: "typed reply" })],
          createdAt: 1,
        });
        const event = AgentStreamEvent.cases.TurnCompleted.make({
          assistantMessage,
          stats: {
            rawMessageCount: 2,
            modelMessageCount: 2,
            estimatedModelTokens: 12,
            compactionCount: 0,
            lastCompactedAt: null,
          },
        });
        const encodedEvent = yield* Schema.encodeUnknownEffect(AgentStreamEvent)(event);
        const decodedEvent = yield* Schema.decodeUnknownEffect(AgentStreamEvent)(encodedEvent);
        expect(decodedEvent).toEqual(event);

        const error = new AgentInferenceError({
          operation: "stream-text",
          message: "provider unavailable",
        });
        const encodedError = yield* Schema.encodeUnknownEffect(AgentRpcError)(error);
        const decodedError = yield* Schema.decodeUnknownEffect(AgentRpcError)(encodedError);
        expect(decodedError._tag).toBe("AgentInferenceError");

        const frame = {
          _tag: "Chunk",
          requestId: "request-1",
          values: [encodedEvent, encodedError],
        };
        const parser = RpcSerialization.ndjson.makeUnsafe();
        const encodedFrame = yield* Schema.decodeUnknownEffect(Schema.String)(parser.encode(frame));
        expect(parser.decode(encodedFrame)).toEqual([frame]);
      }),
    ));
});
