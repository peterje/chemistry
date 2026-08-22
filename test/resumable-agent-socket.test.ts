import { describe, expect, test } from "bun:test";
import { ResumableAgentSocket, runtimeSocketUrl } from "../src/client/resumable-agent-socket.ts";
import { SessionId } from "../src/shared/agent-protocol.ts";

const sessionId = SessionId.make("local-development-session");

describe("runtime socket URL", () => {
  test("connects directly to the fixed local backend port", () => {
    expect(runtimeSocketUrl("http://localhost:1337/", sessionId)).toBe(
      "ws://localhost:1338/ws?sessionId=local-development-session",
    );
    expect(runtimeSocketUrl("http://127.0.0.1:1337/chat", sessionId)).toBe(
      "ws://127.0.0.1:1338/ws?sessionId=local-development-session",
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
