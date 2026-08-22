import { expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { MessageId, SessionId } from "@chemistry/contracts/agent-protocol";
import {
  createInitialSession,
  defaultAgentContext,
  textMessage,
} from "@chemistry/agent-runtime/conversation";
import {
  LegacyStoredSession,
  PersistedSession,
  migrateLegacyStoredSession,
  persistedSessionFromStored,
  storedSessionFromPersisted,
} from "@chemistry/agent-runtime/session-store";

const sessionId = SessionId.make("prompt-storage");

test("session storage encodes chat history with Effect AI Prompt", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const initial = createInitialSession(sessionId);
      const stored = {
        ...initial,
        messages: [
          textMessage(MessageId.make("message-user"), "user", "hello", 1),
          textMessage(MessageId.make("message-assistant"), "assistant", "welcome", 2),
        ],
      };
      const encoded = yield* Schema.encodeEffect(PersistedSession)(
        persistedSessionFromStored(stored),
      );

      expect(encoded).toEqual({
        version: 2,
        sessionId: "prompt-storage",
        context: defaultAgentContext,
        chat: {
          prompt: {
            content: [
              { role: "user", content: "hello", options: {} },
              { role: "assistant", content: "welcome", options: {} },
            ],
          },
          metadata: [
            { id: "message-user", createdAt: 1 },
            { id: "message-assistant", createdAt: 2 },
          ],
        },
        compactions: [],
      });

      const decoded = yield* Schema.decodeUnknownEffect(PersistedSession)(encoded);
      const restored = storedSessionFromPersisted(decoded);
      expect(restored.messages.map((entry) => entry.message.role)).toEqual(["user", "assistant"]);
    }),
  ));

test("legacy custom transcript records migrate into canonical Prompt messages", () => {
  const legacy = Schema.decodeUnknownSync(LegacyStoredSession)({
    sessionId: "prompt-storage",
    context: defaultAgentContext,
    messages: [
      {
        id: "legacy-user",
        role: "user",
        parts: [{ _tag: "Text", text: "legacy hello" }],
        createdAt: 1,
      },
      {
        id: "legacy-assistant",
        role: "assistant",
        parts: [{ _tag: "Text", text: "legacy reply" }],
        createdAt: 2,
      },
    ],
    compactions: [],
  });

  const migrated = migrateLegacyStoredSession(legacy);
  expect(migrated.messages.map((entry) => entry.message.role)).toEqual(["user", "assistant"]);
  expect(migrated.messages[0]?.message.content).toEqual([
    expect.objectContaining({ type: "text", text: "legacy hello" }),
  ]);
});
