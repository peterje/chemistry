import { useEffect, useMemo, useSyncExternalStore } from "react";
import type { SessionId } from "@chemistry/contracts/agent-protocol";
import {
  ResumableAgentSocket,
  type RuntimeSocketSnapshot,
} from "@chemistry/client-runtime/resumable-agent-socket";

/** Socket and immutable state exposed by {@link useResumableAgent}. */
export interface ResumableAgentBinding {
  /** Session-scoped transport adapter. */
  readonly socket: ResumableAgentSocket;
  /** Current replay and recovery diagnostics. */
  readonly snapshot: RuntimeSocketSnapshot;
}

/** Connect React to one session-scoped resumable WebSocket adapter. */
export const useResumableAgent = (sessionId: SessionId): ResumableAgentBinding => {
  const socket = useMemo(() => new ResumableAgentSocket(sessionId), [sessionId]);
  const snapshot = useSyncExternalStore(socket.subscribe, socket.getSnapshot, socket.getSnapshot);

  useEffect(() => {
    socket.connect();
    return () => socket.close();
  }, [socket]);

  return { socket, snapshot };
};
