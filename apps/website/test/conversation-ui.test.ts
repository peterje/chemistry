import { expect, test } from "bun:test";
import { isNearBottom, rememberSessionSnapshot } from "../src/client/conversation-ui.ts";

test("rememberSessionSnapshot keeps the last successful transcript for a session", () => {
  const sessionId = "chat-snapshot-cache";
  const cache = new Map<string, { readonly id: string }>();
  const first = { id: "one" };
  expect(rememberSessionSnapshot(cache, sessionId, undefined)).toBeUndefined();
  expect(rememberSessionSnapshot(cache, sessionId, first)).toBe(first);
  expect(rememberSessionSnapshot(cache, sessionId, undefined)).toBe(first);
});

test("isNearBottom only pins when the user is close to the latest message", () => {
  expect(isNearBottom({ scrollHeight: 1_200, scrollTop: 1_100, clientHeight: 80 })).toBe(true);
  expect(isNearBottom({ scrollHeight: 1_200, scrollTop: 200, clientHeight: 80 })).toBe(false);
});
