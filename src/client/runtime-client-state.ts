import {
  RuntimeClientFrame,
  type DurableStreamEvent,
  type OperationId,
  type RuntimeServerFrame,
  type StreamId,
} from "../shared/agent-protocol.ts";

const MAX_RECENT_EVENTS = 2_048;

/** Browser-visible lifecycle of the resumable WebSocket connection. */
export type RuntimeConnectionStatus =
  | "connecting"
  | "replaying"
  | "live"
  | "recovering"
  | "completed"
  | "interrupted"
  | "failed"
  | "disconnected";

/** Immutable browser diagnostic state for one resumable agent socket. */
export interface RuntimeSocketSnapshot {
  /** Current connection or logical-turn state. */
  readonly status: RuntimeConnectionStatus;
  /** Active or last durable stream. */
  readonly streamId: StreamId | null;
  /** Active or last durable operation. */
  readonly operationId: OperationId | null;
  /** Highest event sequence accepted by the browser. */
  readonly lastSequence: number;
  /** Durable execution checkpoint reported by the server. */
  readonly checkpoint: string | null;
  /** Current bounded recovery attempt. */
  readonly recoveryAttempt: number;
  /** Last durable terminal reason. */
  readonly terminalReason: string | null;
  /** Safe transport or protocol diagnostic. */
  readonly error: string | null;
  /** Bounded events accepted during this browser lifetime. */
  readonly recentEvents: ReadonlyArray<DurableStreamEvent>;
}

/** Transport action selected by the pure runtime client reducer. */
export type RuntimeClientAction = "none" | "reconnect" | "close";

/** Pure state transition plus frames the browser adapter must send. */
export interface RuntimeClientTransition {
  /** Next immutable client snapshot. */
  readonly snapshot: RuntimeSocketSnapshot;
  /** Typed protocol frames emitted by this transition. */
  readonly outbound: ReadonlyArray<typeof RuntimeClientFrame.Type>;
  /** Transport action required after applying the snapshot. */
  readonly action: RuntimeClientAction;
}

/** Construct the initial connecting client state. */
export const initialRuntimeSocketSnapshot = (): RuntimeSocketSnapshot => ({
  status: "connecting",
  streamId: null,
  operationId: null,
  lastSequence: -1,
  checkpoint: null,
  recoveryAttempt: 0,
  terminalReason: null,
  error: null,
  recentEvents: [],
});

/** Apply one decoded server frame with sequence dedupe and gap detection. */
export const applyRuntimeServerFrame = (
  current: RuntimeSocketSnapshot,
  frame: RuntimeServerFrame,
): RuntimeClientTransition => {
  switch (frame._tag) {
    case "ResumeProbe": {
      const streamId = frame.activeStreamId;
      const afterSequence =
        streamId !== null && current.streamId === streamId ? current.lastSequence : -1;
      return {
        snapshot: {
          ...current,
          status: "replaying",
          streamId,
          operationId: frame.runtime.activeOperation?.operationId ?? null,
          lastSequence: afterSequence,
          checkpoint: frame.runtime.activeOperation?.checkpoint ?? null,
          recoveryAttempt: frame.runtime.recoveryAttempt,
          terminalReason: frame.runtime.lastTerminalReason,
          error: null,
        },
        outbound: [
          RuntimeClientFrame.cases.ResumeAck.make({
            probeId: frame.probeId,
            streamId,
            afterSequence,
          }),
        ],
        action: "none",
      };
    }
    case "TurnAccepted":
      return {
        snapshot: {
          ...current,
          status: "live",
          streamId: frame.streamId,
          operationId: frame.operationId,
          lastSequence: -1,
          checkpoint: "admitted",
          terminalReason: null,
          error: null,
          recentEvents: [],
        },
        outbound: [],
        action: "none",
      };
    case "StreamEvent": {
      const event = frame.durableEvent;
      const highWater = current.streamId === event.streamId ? current.lastSequence : -1;
      if (event.sequence <= highWater) {
        return { snapshot: current, outbound: [], action: "none" };
      }
      if (event.sequence !== highWater + 1) {
        return {
          snapshot: { ...current, error: "Sequence gap; reconnecting for replay" },
          outbound: [],
          action: "reconnect",
        };
      }
      return {
        snapshot: {
          ...current,
          status: frame.replay ? "replaying" : "live",
          streamId: event.streamId,
          operationId: event.operationId,
          lastSequence: event.sequence,
          checkpoint: "streaming",
          error: null,
          recentEvents: [...current.recentEvents, event].slice(-MAX_RECENT_EVENTS),
        },
        outbound: [
          RuntimeClientFrame.cases.StreamAck.make({
            streamId: event.streamId,
            sequence: event.sequence,
          }),
        ],
        action: "none",
      };
    }
    case "ResumeComplete":
      return {
        snapshot: {
          ...current,
          status: "live",
          streamId: frame.streamId,
          lastSequence: frame.throughSequence,
          error: null,
        },
        outbound: [],
        action: "none",
      };
    case "Recovering":
      return {
        snapshot: {
          ...current,
          status: "recovering",
          streamId: frame.operation.streamId,
          operationId: frame.operation.operationId,
          checkpoint: frame.operation.checkpoint,
          recoveryAttempt: frame.operation.attempt,
          terminalReason: frame.operation.terminalReason,
          error: null,
        },
        outbound: [],
        action: "none",
      };
    case "StreamTerminal":
      return {
        snapshot: {
          ...current,
          status:
            frame.status === "completed"
              ? "completed"
              : frame.status === "interrupted"
                ? "interrupted"
                : "failed",
          streamId: frame.streamId,
          operationId: frame.operationId,
          lastSequence: frame.sequence,
          checkpoint: "terminal",
          recoveryAttempt: frame.attempt,
          terminalReason: frame.reason,
          error: frame.status === "failed" ? (frame.reason ?? "Turn failed") : null,
        },
        outbound: [],
        action: "none",
      };
    case "ProtocolError":
      return {
        snapshot: { ...current, error: frame.message },
        outbound: [],
        action: frame.recoverable ? "none" : "close",
      };
    case "Pong":
    case "KeepAliveAck":
      return { snapshot: current, outbound: [], action: "none" };
  }
};
