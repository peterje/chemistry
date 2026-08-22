import { describe, expect, test } from "bun:test";
import {
  AgentStreamEvent,
  DurableStreamEvent,
  OperationId,
  StreamId,
} from "@chemistry/contracts/agent-protocol";
import {
  acceptReplayBacklog,
  beginReplayHandoff,
  drainReplayPending,
  parkLiveEvent,
} from "@chemistry/agent-runtime/replay-handoff";

const streamId = StreamId.make("handoff-stream");
const operationId = OperationId.make("handoff-operation");
const event = (sequence: number) =>
  DurableStreamEvent.make({
    streamId,
    operationId,
    sequence,
    event: AgentStreamEvent.cases.TextDelta.make({ delta: String(sequence) }),
    producedAt: sequence,
  });

describe("stored replay to live handoff", () => {
  test("deduplicates the event visible in both backlog and parked live delivery", () => {
    const handoff = beginReplayHandoff(streamId, -1);
    expect(parkLiveEvent(handoff, event(1), 8)).toBe("buffered");
    const backlog = acceptReplayBacklog(handoff, [event(0), event(1)]);
    const pending = drainReplayPending(handoff);
    expect([...backlog, ...pending].map((item) => item.sequence)).toEqual([0, 1]);
    expect(handoff.highWater).toBe(1);
  });

  test("orders out-of-order parked events and filters replayed cursors", () => {
    const handoff = beginReplayHandoff(streamId, 2);
    expect(parkLiveEvent(handoff, event(4), 8)).toBe("buffered");
    expect(parkLiveEvent(handoff, event(3), 8)).toBe("buffered");
    expect(parkLiveEvent(handoff, event(2), 8)).toBe("duplicate");
    expect(drainReplayPending(handoff).map((item) => item.sequence)).toEqual([3, 4]);
  });

  test("parks live events before resume ACK and honors the later browser cursor", () => {
    const handoff = beginReplayHandoff(streamId, -1);
    expect(parkLiveEvent(handoff, event(4), 8)).toBe("buffered");
    handoff.highWater = 3;
    expect(acceptReplayBacklog(handoff, [event(2), event(3)])).toEqual([]);
    expect(drainReplayPending(handoff).map((item) => item.sequence)).toEqual([4]);
  });

  test("signals bounded overflow instead of dropping a durable event", () => {
    const handoff = beginReplayHandoff(streamId, -1);
    expect(parkLiveEvent(handoff, event(0), 1)).toBe("buffered");
    expect(parkLiveEvent(handoff, event(1), 1)).toBe("overflow");
    expect(handoff.pending.map((item) => item.sequence)).toEqual([0]);
  });
});
