import { describe, expect, test } from "bun:test";
import {
  AgentStreamEvent,
  BootId,
  ConnectionId,
  DurableStreamEvent,
  OperationId,
  RuntimeOperationSummary,
  RuntimeServerFrame,
  RuntimeSnapshot,
  SessionId,
  StreamId,
  SubmissionId,
} from "../src/shared/agent-protocol.ts";
import {
  applyRuntimeServerFrame,
  initialRuntimeSocketSnapshot,
} from "../src/client/runtime-client-state.ts";

const streamId = StreamId.make("client-stream");
const operationId = OperationId.make("client-operation");
const operation = RuntimeOperationSummary.make({
  operationId,
  submissionId: SubmissionId.make("client-submission"),
  streamId,
  status: "running",
  checkpoint: "streaming",
  generation: 1,
  attempt: 0,
  progress: 2,
  recoveryWork: 0,
  terminalReason: null,
  updatedAt: 1,
});
const runtime = RuntimeSnapshot.make({
  bootId: BootId.make("client-boot"),
  activeOperation: operation,
  queueDepth: 0,
  retainedStreamCount: 1,
  recoveryAttempt: 0,
  lastTerminalReason: null,
});
const event = (sequence: number, delta: string) =>
  DurableStreamEvent.make({
    streamId,
    operationId,
    sequence,
    event: AgentStreamEvent.cases.TextDelta.make({ delta }),
    producedAt: sequence,
  });

describe("resumable browser runtime reducer", () => {
  test("uses the validated high-water sequence on reconnect", () => {
    const current = {
      ...initialRuntimeSocketSnapshot(),
      streamId,
      lastSequence: 4,
    };
    const transition = applyRuntimeServerFrame(
      current,
      RuntimeServerFrame.cases.ResumeProbe.make({
        probeId: "probe-1",
        connectionId: ConnectionId.make("connection-1"),
        sessionId: SessionId.make("client-session"),
        activeStreamId: streamId,
        latestSequence: 6,
        runtime,
      }),
    );
    expect(transition.snapshot.status).toBe("replaying");
    expect(transition.snapshot.lastSequence).toBe(4);
    expect(transition.outbound[0]).toEqual({
      _tag: "ResumeAck",
      probeId: "probe-1",
      streamId,
      afterSequence: 4,
    });
  });

  test("deduplicates overlap and reconnects rather than accepting a gap", () => {
    const accepted = applyRuntimeServerFrame(
      { ...initialRuntimeSocketSnapshot(), streamId },
      RuntimeServerFrame.cases.StreamEvent.make({
        durableEvent: event(0, "a"),
        replay: true,
      }),
    );
    const duplicate = applyRuntimeServerFrame(
      accepted.snapshot,
      RuntimeServerFrame.cases.StreamEvent.make({
        durableEvent: event(0, "a"),
        replay: false,
      }),
    );
    expect(duplicate.snapshot).toBe(accepted.snapshot);
    expect(duplicate.outbound).toEqual([]);

    const gap = applyRuntimeServerFrame(
      accepted.snapshot,
      RuntimeServerFrame.cases.StreamEvent.make({
        durableEvent: event(2, "c"),
        replay: false,
      }),
    );
    expect(gap.action).toBe("reconnect");
    expect(gap.snapshot.lastSequence).toBe(0);
  });

  test("reconstructs a logical response from a full replay after reload", () => {
    let snapshot = initialRuntimeSocketSnapshot();
    const probe = applyRuntimeServerFrame(
      snapshot,
      RuntimeServerFrame.cases.ResumeProbe.make({
        probeId: "probe-reload",
        connectionId: ConnectionId.make("connection-reload"),
        sessionId: SessionId.make("client-reload"),
        activeStreamId: streamId,
        latestSequence: 1,
        runtime,
      }),
    );
    expect(probe.outbound[0]).toMatchObject({ afterSequence: -1 });
    snapshot = probe.snapshot;
    snapshot = applyRuntimeServerFrame(
      snapshot,
      RuntimeServerFrame.cases.StreamEvent.make({
        durableEvent: event(0, "hello "),
        replay: true,
      }),
    ).snapshot;
    snapshot = applyRuntimeServerFrame(
      snapshot,
      RuntimeServerFrame.cases.StreamEvent.make({
        durableEvent: event(1, "again"),
        replay: true,
      }),
    ).snapshot;
    const text = snapshot.recentEvents
      .filter((item) => item.event._tag === "TextDelta")
      .map((item) => (item.event._tag === "TextDelta" ? item.event.delta : ""))
      .join("");
    expect(text).toBe("hello again");
    expect(snapshot.lastSequence).toBe(1);
  });

  test("surfaces recovering and durable terminal states", () => {
    const recovering = applyRuntimeServerFrame(
      initialRuntimeSocketSnapshot(),
      RuntimeServerFrame.cases.Recovering.make({
        operation: RuntimeOperationSummary.make({
          ...operation,
          status: "recovering",
          attempt: 2,
        }),
      }),
    );
    expect(recovering.snapshot.status).toBe("recovering");
    expect(recovering.snapshot.recoveryAttempt).toBe(2);

    const failed = applyRuntimeServerFrame(
      recovering.snapshot,
      RuntimeServerFrame.cases.StreamTerminal.make({
        streamId,
        operationId,
        status: "failed",
        sequence: 5,
        generation: 3,
        attempt: 2,
        recoveryWork: 4,
        reason: "recovery-attempts-exhausted",
      }),
    );
    expect(failed.snapshot.status).toBe("failed");
    expect(failed.snapshot.terminalReason).toBe("recovery-attempts-exhausted");
  });
});
