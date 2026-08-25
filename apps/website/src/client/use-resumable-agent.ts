import { useEffect, useMemo, useSyncExternalStore } from "react";
import type { SessionId } from "@chemistry/contracts/agent-protocol";
import {
  ResumableAgentSocket,
  type RuntimeSocketSnapshot,
} from "@chemistry/client-runtime/resumable-agent-socket";

const MAX_CACHED_RUNTIME_SOCKETS = 8;
const runtimeSockets = new Map<string, ResumableAgentSocket>();
const runtimeSocketOrder: Array<string> = [];

/** Socket and immutable state exposed by {@link useResumableAgent}. */
export interface ResumableAgentBinding {
  /** Session-scoped transport adapter. */
  readonly socket: ResumableAgentSocket;
  /** Current replay and recovery diagnostics. */
  readonly snapshot: RuntimeSocketSnapshot;
}

const retainRuntimeSocket = (
  sessionId: string,
  socket: ResumableAgentSocket,
): ResumableAgentSocket => {
  runtimeSockets.set(sessionId, socket);
  const existing = runtimeSocketOrder.indexOf(sessionId);
  if (existing >= 0) runtimeSocketOrder.splice(existing, 1);
  runtimeSocketOrder.push(sessionId);
  while (runtimeSocketOrder.length > MAX_CACHED_RUNTIME_SOCKETS) {
    const oldest = runtimeSocketOrder.shift();
    if (oldest === undefined || oldest === sessionId) continue;
    runtimeSockets.get(oldest)?.close();
    runtimeSockets.delete(oldest);
  }
  return socket;
};

const runtimeSocketForSession = (sessionId: SessionId): ResumableAgentSocket => {
  const cached = runtimeSockets.get(sessionId);
  if (cached !== undefined) return retainRuntimeSocket(sessionId, cached);
  return retainRuntimeSocket(sessionId, new ResumableAgentSocket(sessionId));
};

/** Connect React to one session-scoped resumable WebSocket adapter. */
export const useResumableAgent = (sessionId: SessionId): ResumableAgentBinding => {
  const socket = useMemo(() => runtimeSocketForSession(sessionId), [sessionId]);
  const snapshot = useSyncExternalStore(socket.subscribe, socket.getSnapshot, socket.getSnapshot);

  useEffect(() => {
    socket.connect();
  }, [socket]);

  return { socket, snapshot };
};
