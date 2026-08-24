import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import {
  RuntimeClientFrame,
  SubmissionId,
  type SessionId,
  type SubmissionId as SubmissionIdType,
} from "@chemistry/contracts/agent-protocol";
import { LOCAL_AGENT_BACKEND_PORT } from "@chemistry/contracts/local-development";
import {
  decodeRuntimeServerFrame,
  encodeRuntimeClientFrame,
} from "@chemistry/contracts/runtime-protocol";
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
const LOCAL_HOSTNAMES = new Set(["localhost", "127.0.0.1", "[::1]", "::1"]);

/** Build the runtime socket URL, bypassing the local Website worker's unsupported upgrade proxy. */
export const runtimeSocketUrl = (locationHref: string, sessionId: SessionId): string => {
  const url = new URL("/ws", locationHref);
  if (LOCAL_HOSTNAMES.has(url.hostname)) url.port = String(LOCAL_AGENT_BACKEND_PORT);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("sessionId", sessionId);
  return url.toString();
};

/** Browser adapter implementing bounded reconnect, resume, replay dedupe, and liveness. */
export class ResumableAgentSocket {
  readonly #sessionId: SessionId;
  readonly #listeners = new Set<() => void>();
  #socket: WebSocket | undefined;
  #snapshot = initialRuntimeSocketSnapshot();
  #closed = false;
  #reconnectAttempt = 0;
  #connectTimer: number | undefined;
  #reconnectTimer: number | undefined;
  #pingTimer: number | undefined;
  #applyQueue: Promise<void> = Promise.resolve();
  #sendQueue: Promise<void> = Promise.resolve();
  #onlineListening = false;

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

  /** Open the hibernatable WebSocket and begin resume negotiation. */
  connect = (): void => {
    if (this.#socket !== undefined || this.#connectTimer !== undefined) return;
    this.#closed = false;
    this.#listenForOnline();
    this.#setSnapshot({ ...this.#snapshot, status: "connecting", error: null });
    this.#connectTimer = window.setTimeout(() => {
      this.#connectTimer = undefined;
      if (this.#closed || this.#socket !== undefined) return;
      const socket = new WebSocket(runtimeSocketUrl(window.location.href, this.#sessionId));
      this.#socket = socket;
      socket.addEventListener("message", (event) => {
        if (this.#socket !== socket) return;
        const encoded = Schema.decodeUnknownResult(Schema.String)(event.data);
        if (Result.isFailure(encoded) || encoded.success.length === 0) return;
        this.#enqueueApply(() => this.#handleMessage(socket, encoded.success));
      });
      socket.addEventListener("close", () => {
        if (this.#socket !== socket) return;
        this.#socket = undefined;
        this.#clearPing();
        if (!this.#closed) this.#scheduleReconnect();
      });
      socket.addEventListener("error", () => {
        if (this.#socket !== socket) return;
        this.#setSnapshot({ ...this.#snapshot, error: "WebSocket transport error" });
      });
    }, 0);
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

  /** Close the current connection and timers; a later {@link connect} starts it again. */
  close = (): void => {
    this.#closed = true;
    if (this.#connectTimer !== undefined) window.clearTimeout(this.#connectTimer);
    this.#connectTimer = undefined;
    if (this.#reconnectTimer !== undefined) window.clearTimeout(this.#reconnectTimer);
    this.#reconnectTimer = undefined;
    this.#reconnectAttempt = 0;
    this.#unlistenOnline();
    this.#clearPing();
    const socket = this.#socket;
    this.#socket = undefined;
    socket?.close(1000, "session-changed");
    this.#setSnapshot({ ...this.#snapshot, status: "disconnected" });
  };

  async #handleMessage(socket: WebSocket, encoded: string): Promise<void> {
    const decoded = await Effect.runPromise(decodeRuntimeServerFrame(encoded).pipe(Effect.result));
    if (this.#socket !== socket) return;
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

  #enqueueApply(task: () => Promise<void>): void {
    this.#applyQueue = this.#applyQueue.then(task, task);
  }

  #send(frame: RuntimeClientFrame): void {
    const socket = this.#socket;
    if (socket?.readyState !== WebSocket.OPEN) return;
    this.#sendQueue = this.#sendQueue.then(
      () => this.#flush(socket, frame),
      () => this.#flush(socket, frame),
    );
  }

  async #flush(socket: WebSocket, frame: RuntimeClientFrame): Promise<void> {
    if (socket.readyState !== WebSocket.OPEN) return;
    const encoded = await Effect.runPromise(encodeRuntimeClientFrame(frame).pipe(Effect.result));
    if (Result.isFailure(encoded)) {
      this.#fail(encoded.failure.message);
      return;
    }
    if (socket.readyState === WebSocket.OPEN) socket.send(encoded.success);
  }

  #scheduleReconnect(): void {
    this.#listenForOnline();
    if (globalThis.navigator?.onLine === false) {
      this.#setSnapshot({
        ...this.#snapshot,
        status: "interrupted",
        error: "Network is offline",
      });
      return;
    }
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
      if (this.#closed) return;
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
    const terminal = this.#snapshot.status === "completed" || this.#snapshot.status === "failed";
    this.#setSnapshot({
      ...this.#snapshot,
      status: terminal ? this.#snapshot.status : "interrupted",
      error: message,
    });
    this.#socket?.close(1008, "protocol-failure");
  }

  #onOnline = (): void => {
    if (this.#closed) return;
    this.#reconnectAttempt = 0;
    if (this.#reconnectTimer !== undefined) {
      window.clearTimeout(this.#reconnectTimer);
      this.#reconnectTimer = undefined;
    }
    if (this.#socket === undefined && this.#connectTimer === undefined) this.connect();
  };

  #listenForOnline(): void {
    if (this.#onlineListening) return;
    window.addEventListener("online", this.#onOnline);
    this.#onlineListening = true;
  }

  #unlistenOnline(): void {
    if (!this.#onlineListening) return;
    window.removeEventListener("online", this.#onOnline);
    this.#onlineListening = false;
  }

  #setSnapshot(snapshot: RuntimeSocketSnapshot): void {
    this.#snapshot = snapshot;
    for (const listener of this.#listeners) listener();
  }
}
