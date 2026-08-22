import { describe, expect, test } from "bun:test";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ConnectionId, SessionId, StreamId } from "@chemistry/contracts/agent-protocol";
import {
  RuntimeSocketAttachment,
  applyRuntimeStreamAck,
} from "@chemistry/backend/runtime-websocket-adapter";

describe("hibernatable socket attachment", () => {
  test("rehydrates typed session, probe, stream, and cursor state", () => {
    const attachment = RuntimeSocketAttachment.make({
      version: 1,
      connectionId: ConnectionId.make("attachment-connection"),
      sessionId: SessionId.make("attachment-session"),
      probeId: "attachment-probe",
      probedStreamId: StreamId.make("attachment-stream"),
      sentStreamId: StreamId.make("attachment-stream"),
      sentSequence: 7,
      acknowledgedStreamId: StreamId.make("attachment-stream"),
      acknowledgedSequence: 7,
    });
    expect(
      Schema.decodeUnknownResult(RuntimeSocketAttachment)(structuredClone(attachment)),
    ).toEqual(Result.succeed(attachment));
  });

  test("advances ACKs monotonically and rejects unsent or stale streams", () => {
    const streamId = StreamId.make("ack-stream");
    const attachment = RuntimeSocketAttachment.make({
      version: 1,
      connectionId: ConnectionId.make("ack-connection"),
      sessionId: SessionId.make("ack-session"),
      probeId: "ack-probe",
      probedStreamId: streamId,
      sentStreamId: streamId,
      sentSequence: 7,
      acknowledgedStreamId: streamId,
      acknowledgedSequence: 5,
    });
    expect(applyRuntimeStreamAck(attachment, streamId, 6)?.acknowledgedSequence).toBe(6);
    expect(applyRuntimeStreamAck(attachment, streamId, 4)?.acknowledgedSequence).toBe(5);
    expect(applyRuntimeStreamAck(attachment, streamId, 8)).toBeUndefined();
    expect(applyRuntimeStreamAck(attachment, StreamId.make("other-stream"), 6)).toBeUndefined();
  });

  test("rejects stale versions and malformed cursors", () => {
    expect(
      Schema.decodeUnknownResult(RuntimeSocketAttachment)({
        version: 2,
        connectionId: "attachment-connection",
        sessionId: "attachment-session",
        probeId: "attachment-probe",
        probedStreamId: null,
        sentStreamId: null,
        sentSequence: -1,
        acknowledgedStreamId: null,
        acknowledgedSequence: -1,
      }),
    ).toMatchObject({ _tag: "Failure" });
    expect(
      Schema.decodeUnknownResult(RuntimeSocketAttachment)({
        version: 1,
        connectionId: "attachment-connection",
        sessionId: "attachment-session",
        probeId: "attachment-probe",
        probedStreamId: null,
        sentStreamId: null,
        sentSequence: -1,
        acknowledgedStreamId: null,
        acknowledgedSequence: 0.5,
      }),
    ).toMatchObject({ _tag: "Failure" });
  });
});
