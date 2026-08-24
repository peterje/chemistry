import { describe, expect, test } from "bun:test";
import {
  ResumableAgentSocket,
  runtimeSocketUrl,
} from "@chemistry/client-runtime/resumable-agent-socket";
import { SessionId } from "@chemistry/contracts/agent-protocol";

const sessionId = SessionId.make("local-development-session");

describe("runtime socket URL", () => {
  test("connects directly to the fixed local backend port", () => {
    expect(runtimeSocketUrl("http://localhost:1337/", sessionId)).toBe(
      "ws://localhost:1338/ws?sessionId=local-development-session",
    );
    expect(runtimeSocketUrl("http://127.0.0.1:1337/chat", sessionId)).toBe(
      "ws://127.0.0.1:1338/ws?sessionId=local-development-session",
    );
    expect(runtimeSocketUrl("http://[::1]:1337/", sessionId)).toBe(
      "ws://[::1]:1338/ws?sessionId=local-development-session",
    );
  });

  test("keeps deployed WebSockets same-origin", () => {
    expect(runtimeSocketUrl("https://chemistry.example/app", sessionId)).toBe(
      "wss://chemistry.example/ws?sessionId=local-development-session",
    );
  });

  test.serial("survives the React Strict Mode setup-cleanup-setup cycle", async () => {
    const previousWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
    const previousWebSocket = Object.getOwnPropertyDescriptor(globalThis, "WebSocket");
    const openedUrls: Array<string> = [];

    class FakeWebSocket {
      static readonly OPEN = 1;
      readonly readyState = 0;

      constructor(url: string) {
        openedUrls.push(url);
      }

      addEventListener(): void {}
      close(): void {}
      send(): void {}
    }

    Object.defineProperty(globalThis, "window", {
      configurable: true,
      value: {
        location: { href: "http://localhost:1337/" },
        setTimeout,
        clearTimeout,
        setInterval,
        clearInterval,
        addEventListener() {},
        removeEventListener() {},
      },
    });
    Object.defineProperty(globalThis, "WebSocket", {
      configurable: true,
      value: FakeWebSocket,
    });

    try {
      const socket = new ResumableAgentSocket(sessionId);
      socket.connect();
      expect(openedUrls).toEqual([]);
      socket.close();
      socket.connect();
      await Bun.sleep(10);
      expect(openedUrls).toEqual(["ws://localhost:1338/ws?sessionId=local-development-session"]);
      expect(socket.getSnapshot().status).toBe("connecting");
      socket.close();
    } finally {
      if (previousWindow === undefined) Reflect.deleteProperty(globalThis, "window");
      else Object.defineProperty(globalThis, "window", previousWindow);
      if (previousWebSocket === undefined) Reflect.deleteProperty(globalThis, "WebSocket");
      else Object.defineProperty(globalThis, "WebSocket", previousWebSocket);
    }
  });
});

test.serial("reconnects when the browser comes back online", async () => {
  const previousWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  const previousWebSocket = Object.getOwnPropertyDescriptor(globalThis, "WebSocket");
  const previousNavigator = Object.getOwnPropertyDescriptor(globalThis, "navigator");
  const openedUrls: Array<string> = [];
  const onlineListeners: Array<() => void> = [];
  let onLine = false;

  class FakeWebSocket {
    static readonly OPEN = 1;
    readyState = 0;
    readonly #listeners = new Map<string, () => void>();

    constructor(url: string) {
      openedUrls.push(url);
      queueMicrotask(() => {
        this.readyState = 3;
        this.#listeners.get("close")?.();
      });
    }

    addEventListener(type: string, listener: () => void): void {
      this.#listeners.set(type, listener);
    }

    close(): void {
      this.readyState = 3;
      this.#listeners.get("close")?.();
    }

    send(): void {}
  }

  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: {
      location: { href: "http://localhost:1337/" },
      setTimeout,
      clearTimeout,
      setInterval,
      clearInterval,
      addEventListener(type: string, listener: () => void) {
        if (type === "online") onlineListeners.push(listener);
      },
      removeEventListener(type: string, listener: () => void) {
        const index = onlineListeners.indexOf(listener);
        if (index >= 0) onlineListeners.splice(index, 1);
      },
    },
  });
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: {
      get onLine() {
        return onLine;
      },
    },
  });
  Object.defineProperty(globalThis, "WebSocket", {
    configurable: true,
    value: FakeWebSocket,
  });

  try {
    const socket = new ResumableAgentSocket(sessionId);
    socket.connect();
    await Bun.sleep(20);
    expect(openedUrls).toHaveLength(1);
    expect(socket.getSnapshot().status).toBe("interrupted");
    onLine = true;
    for (const listener of onlineListeners) listener();
    await Bun.sleep(20);
    expect(openedUrls).toHaveLength(2);
    expect(socket.getSnapshot().status).toBe("connecting");
    socket.close();
  } finally {
    if (previousWindow === undefined) Reflect.deleteProperty(globalThis, "window");
    else Object.defineProperty(globalThis, "window", previousWindow);
    if (previousWebSocket === undefined) Reflect.deleteProperty(globalThis, "WebSocket");
    else Object.defineProperty(globalThis, "WebSocket", previousWebSocket);
    if (previousNavigator === undefined) Reflect.deleteProperty(globalThis, "navigator");
    else Object.defineProperty(globalThis, "navigator", previousNavigator);
  }
});
