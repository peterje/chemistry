import type { DurableStreamEvent, StreamId } from "../shared/agent-protocol.ts";

/** Ephemeral cursor and parking buffer used only during stored-to-live handoff. */
export interface ReplayHandoff {
  /** Stream whose backlog is being drained. */
  readonly streamId: StreamId;
  /** Highest sequence already emitted to this connection. */
  highWater: number;
  /** Live events parked while the durable backlog is read. */
  readonly pending: Array<DurableStreamEvent>;
}

/** Result of trying to park one concurrently published live event. */
export type ReplayParkResult = "duplicate" | "buffered" | "overflow";

/** Begin a replay handoff before reading durable backlog. */
export const beginReplayHandoff = (streamId: StreamId, afterSequence: number): ReplayHandoff => ({
  streamId,
  highWater: afterSequence,
  pending: [],
});

/** Park one live event while replay drains, without dropping a durable event silently. */
export const parkLiveEvent = (
  handoff: ReplayHandoff,
  event: DurableStreamEvent,
  capacity: number,
): ReplayParkResult => {
  if (event.streamId !== handoff.streamId || event.sequence <= handoff.highWater) {
    return "duplicate";
  }
  if (handoff.pending.length >= capacity) return "overflow";
  handoff.pending.push(event);
  return "buffered";
};

/** Select a strictly increasing backlog and advance the shared high-water cursor. */
export const acceptReplayBacklog = (
  handoff: ReplayHandoff,
  events: ReadonlyArray<DurableStreamEvent>,
): ReadonlyArray<DurableStreamEvent> => {
  const accepted: Array<DurableStreamEvent> = [];
  for (const event of [...events].sort((left, right) => left.sequence - right.sequence)) {
    if (event.streamId !== handoff.streamId || event.sequence <= handoff.highWater) continue;
    accepted.push(event);
    handoff.highWater = event.sequence;
  }
  return accepted;
};

/** Drain parked live events through the same cursor used by stored replay. */
export const drainReplayPending = (handoff: ReplayHandoff): ReadonlyArray<DurableStreamEvent> => {
  const pending = handoff.pending.splice(0);
  return acceptReplayBacklog(handoff, pending);
};
