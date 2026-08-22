import { expect, test } from "bun:test";
import { SessionId } from "@chemistry/contracts/agent-protocol";
import {
  LegacyChatCatalogStateV1,
  MAX_CATALOG_CHATS,
  createChat,
  emptyChatCatalog,
  listChats,
  migrateChatCatalogStateV1,
  titleFromFirstPrompt,
  touchChat,
} from "../src/chat-catalog.ts";

test("catalog creation is idempotent and first activity sets a stable title", () => {
  const sessionId = SessionId.make("chat-alpha");
  const [createdState, created] = createChat(emptyChatCatalog(), sessionId, 10);
  expect(created.title).toBe("New chat");
  const [duplicateState] = createChat(createdState, sessionId, 20);
  expect(duplicateState).toEqual(createdState);

  const [touchedState, touched] = touchChat(
    createdState,
    sessionId,
    "  Durable   replay works  ",
    30,
  );
  expect(touched.title).toBe("Durable replay works");
  const [, touchedAgain] = touchChat(touchedState, sessionId, "A replacement title", 40);
  expect(touchedAgain.title).toBe("Durable replay works");
  expect(touchedAgain.createdAt).toBe(10);
  expect(touchedAgain.updatedAt).toBe(40);
});

test("a first prompt equal to the placeholder remains immutable", () => {
  const sessionId = SessionId.make("chat-placeholder-title");
  const [created] = createChat(emptyChatCatalog(), sessionId, 10);
  const [firstState, first] = touchChat(created, sessionId, "New chat", 20);
  const [, second] = touchChat(firstState, sessionId, "Second prompt", 30);
  expect(first.title).toBe("New chat");
  expect(second.title).toBe("New chat");
});

test("legacy catalog migration preserves every existing title as immutable", () => {
  const sessionId = SessionId.make("chat-legacy-title");
  const legacy = LegacyChatCatalogStateV1.make({
    version: 1,
    chats: [
      {
        sessionId,
        title: "New chat",
        createdAt: 10,
        updatedAt: 20,
      },
    ],
  });
  const migrated = migrateChatCatalogStateV1(legacy);
  const [, touched] = touchChat(migrated, sessionId, "Replacement", 30);
  expect(touched.title).toBe("New chat");
});

test("catalog orders recent activity and remains bounded", () => {
  let state = emptyChatCatalog();
  for (let index = 0; index < MAX_CATALOG_CHATS + 4; index += 1) {
    [state] = touchChat(state, SessionId.make(`chat-${index}`), `Prompt ${index}`, index);
  }
  const listed = listChats(state);
  expect(listed.chats).toHaveLength(MAX_CATALOG_CHATS);
  expect(String(listed.chats[0]?.sessionId)).toBe(`chat-${MAX_CATALOG_CHATS + 3}`);
  expect(String(listed.chats.at(-1)?.sessionId)).toBe("chat-4");
});

test("long titles are compact and preserve a readable word boundary", () => {
  const title = titleFromFirstPrompt(
    "Explain why append-before-publish guarantees protect every reconnecting browser subscriber",
  );
  expect(title.length).toBeLessThanOrEqual(56);
  expect(title.endsWith("…")).toBe(true);
});
