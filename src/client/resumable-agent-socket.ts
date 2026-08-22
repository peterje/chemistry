import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import {
  RuntimeClientFrame,
  SubmissionId,
  type SessionId,
  type SubmissionId as SubmissionIdType,
} from "../shared/agent-protocol.ts";
import { decodeRuntimeServerFrame, encodeRuntimeClientFrame } from "../shared/runtime-protocol.ts";
import {
  applyRuntimeServerFrame,
  initialRuntimeSocketSnapshot,
  type RuntimeSocketSnapshot,
} from "./runtime-client-state.ts";

/** Browser runtime state types re-exported beside the socket adapter. */
export type { RuntimeConnectionStatus, RuntimeSocketSnapshot } from "./runtime-client-state.ts";

const MAX_RECONNECT_ATTEMPTS = 8;
const MAX_RECONNECT_DELAY_MS = 10_000;
const PING_INTERVAL_MS = 20_000;

/** Browser adapter implementing bounded reconnect, resume, replay dedupe, and liveness. */
export class ResumableAgentSocket {
  readonly #sessionId: SessionId;
  readonly #listeners = new Set<() => void>();
  #socket: WebSocket | undefined;
  #snapshot = initialRuntimeSocketSnapshot();
  #closed = false;
  #reconnectAttempt = 0;
  #reconnectTimer: number | undefined;
  #pingTimer: number | undefined;

  /** Construct a disconnected adapter for one durable session. */
  constructor(sessionId: SessionId) {
    this.#sessionId = sessionId;
  }

  /** Read the stable immutable snapshot consumed by React. */
  getSnapshot = (): RuntimeSocketSnapshot => this.#snapshot;

  /** Subscribe to snapshot changes. */
  subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  /** Open the same-origin hibernatable WebSocket and begin resume negotiation. */
  connect = (): void => {
    if (this.#closed || this.#socket !== undefined) return;
    this.#setSnapshot({ ...this.#snapshot, status: "connecting", error: null });
    const url = new URL("/ws", window.location.href);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("sessionId", this.#sessionId);
    const socket = new WebSocket(url);
    this.#socket = socket;
    socket.addEventListener("message", (event) => {
      const encoded = Schema.decodeUnknownResult(Schema.String)(event.data);
      if (Result.isFailure(encoded)) {
        this.#fail("Received a binary runtime frame");
        return;
      }
      void this.#handleMessage(encoded.success);
    });
    socket.addEventListener("close", () => {
      if (this.#socket === socket) this.#socket = undefined;
      this.#clearPing();
      if (!this.#closed) this.#scheduleReconnect();
    });
    socket.addEventListener("error", () => {
      this.#setSnapshot({ ...this.#snapshot, error: "WebSocket transport error" });
    });
  };

  /** Submit one idempotent logical turn when the connection is open. */
  submit = (prompt: string): SubmissionIdType | undefined => {
    if (this.#socket?.readyState !== WebSocket.OPEN) {
      this.#setSnapshot({ ...this.#snapshot, error: "Runtime connection is not open" });
      return undefined;
    }
    const submissionId = SubmissionId.make(`submission-${crypto.randomUUID()}`);
    this.#send(RuntimeClientFrame.cases.SubmitTurn.make({ submissionId, prompt }));
    return submissionId;
  };

  /** Permanently close this adapter and cancel reconnect/liveness timers. */
  close = (): void => {
    this.#closed = true;
    if (this.#reconnectTimer !== undefined) window.clearTimeout(this.#reconnectTimer);
    this.#reconnectTimer = undefined;
    this.#clearPing();
    this.#socket?.close(1000, "session-changed");
    this.#socket = undefined;
    this.#setSnapshot({ ...this.#snapshot, status: "disconnected" });
  };

  async #handleMessage(encoded: string): Promise<void> {
    const decoded = await Effect.runPromise(decodeRuntimeServerFrame(encoded).pipe(Effect.result));
    if (Result.isFailure(decoded)) {
      this.#fail(decoded.failure.message);
      return;
    }
    this.#acceptFrame(decoded.success);
  }

  #acceptFrame(frame: Parameters<typeof applyRuntimeServerFrame>[1]): void {
    const transition = applyRuntimeServerFrame(this.#snapshot, frame);
    if (transition.snapshot !== this.#snapshot) this.#setSnapshot(transition.snapshot);
    for (const outbound of transition.outbound) this.#send(outbound);
    if (frame._tag === "ResumeComplete") {
      this.#reconnectAttempt = 0;
      this.#startPing();
    }
    if (transition.action === "reconnect") {
      this.#socket?.close(1012, "sequence-gap");
    } else if (transition.action === "close") {
      this.#socket?.close(1008, "protocol-failure");
    }
  }

  #send(frame: RuntimeClientFrame): void {
    const socket = this.#socket;
    if (socket?.readyState !== WebSocket.OPEN) return;
    void Effect.runPromise(encodeRuntimeClientFrame(frame).pipe(Effect.result)).then((encoded) => {
      if (Result.isFailure(encoded)) {
        this.#fail(encoded.failure.message);
        return;
      }
      if (socket.readyState === WebSocket.OPEN) socket.send(encoded.success);
    });
  }

  #scheduleReconnect(): void {
    if (this.#reconnectAttempt >= MAX_RECONNECT_ATTEMPTS) {
      this.#setSnapshot({
        ...this.#snapshot,
        status: "disconnected",
        error: `Reconnect budget exhausted after ${MAX_RECONNECT_ATTEMPTS} attempts`,
      });
      return;
    }
    const delay = Math.min(250 * 2 ** this.#reconnectAttempt, MAX_RECONNECT_DELAY_MS);
    this.#reconnectAttempt += 1;
    this.#setSnapshot({ ...this.#snapshot, status: "connecting" });
    this.#reconnectTimer = window.setTimeout(() => {
      this.#reconnectTimer = undefined;
      this.connect();
    }, delay);
  }

  #startPing(): void {
    this.#clearPing();
    this.#pingTimer = window.setInterval(() => {
      this.#send(RuntimeClientFrame.cases.KeepAlive.make({}));
    }, PING_INTERVAL_MS);
  }

  #clearPing(): void {
    if (this.#pingTimer !== undefined) window.clearInterval(this.#pingTimer);
    this.#pingTimer = undefined;
  }

  #fail(message: string): void {
    this.#setSnapshot({ ...this.#snapshot, status: "failed", error: message });
    this.#socket?.close(1008, "protocol-failure");
  }

  #setSnapshot(snapshot: RuntimeSocketSnapshot): void {
    this.#snapshot = snapshot;
    for (const listener of this.#listeners) listener();
  }
}
