import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import type * as Prompt from "effect/unstable/ai/Prompt";
import { AgentStreamEvent, SessionId, chatMessages } from "@chemistry/contracts/agent-protocol";
import { AgentService } from "@chemistry/agent-runtime/agent-service";
import { AgentServiceLive } from "@chemistry/agent-runtime/agent-service-live";
import {
  DeterministicMessageIds,
  InMemorySessionStore,
  TestLanguageModel,
  makeTestLanguageModel,
} from "./support/test-layers.ts";

const makeRuntime = (modelLayer: ReturnType<typeof makeTestLanguageModel>) => {
  const dependencies = Layer.mergeAll(InMemorySessionStore, DeterministicMessageIds, modelLayer);
  return Layer.merge(AgentServiceLive.pipe(Layer.provide(dependencies)), modelLayer);
};

const isCompactionRequest = (
  request: Parameters<Parameters<typeof makeTestLanguageModel>[0]>[0],
): boolean => {
  const first = request.prompt.content[0];
  return first?.role === "system" && first.content.includes("You compact conversation history");
};

const messageParts = (message: Prompt.Message): ReadonlyArray<Prompt.Part> =>
  message.role === "system" ? [] : message.content;

const send = (agent: AgentService["Service"], sessionId: SessionId, text: string) =>
  agent.sendMessage(sessionId, text).pipe(Stream.runDrain);

describe("non-destructive chat compaction", () => {
  test("persists a summary overlay, preserves raw history, and rehydrates it", () => {
    const responseText = "R".repeat(180);
    const model = makeTestLanguageModel((request) =>
      isCompactionRequest(request)
        ? [{ type: "text" as const, text: "Durable concise summary." }]
        : [{ type: "text" as const, text: responseText }],
    );
    const runtime = makeRuntime(model);

    return Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* AgentService;
        const observedModel = yield* TestLanguageModel;
        const sessionId = SessionId.make("manual-compaction");

        for (let index = 0; index < 5; index += 1) {
          yield* send(agent, sessionId, `turn-${index}-${"U".repeat(150)}`);
        }

        const before = yield* agent.getSession(sessionId);
        const beforeMessages = chatMessages(before.chat);
        expect(beforeMessages).toHaveLength(10);
        expect(before.compactions).toHaveLength(0);

        const result = yield* agent.compactSession(sessionId);
        expect(result.compacted).toBe(true);
        expect(result.reason).toBe("manual");

        const after = yield* agent.getSession(sessionId);
        const afterMessages = chatMessages(after.chat);
        expect(afterMessages).toEqual(beforeMessages);
        expect(after.compactions).toHaveLength(1);
        expect(after.compactions[0]?.summary).toBe("Durable concise summary.");
        expect(after.stats.rawMessageCount).toBe(10);
        expect(after.stats.modelMessageCount).toBe(7);
        expect(after.stats.estimatedModelTokens).toBeLessThan(before.stats.estimatedModelTokens);
        expect(after.compactions[0]?.toMessageId).toBe(beforeMessages[3]?.id);
        expect(afterMessages.slice(-6)).toEqual(beforeMessages.slice(-6));

        yield* send(agent, sessionId, "Use the rehydrated summary.");
        const requests = yield* observedModel.requests();
        const lastRequest = requests.at(-1);
        expect(
          lastRequest?.prompt.content.some(
            (message) =>
              message.role === "system" && message.content.includes("<conversation-summary>"),
          ),
        ).toBe(true);
        expect(
          lastRequest?.prompt.content.some(
            (message) =>
              message.role === "user" &&
              message.content.some(
                (part) => part.type === "text" && part.text.startsWith("turn-0-"),
              ),
          ),
        ).toBe(false);
      }).pipe(Effect.provide(runtime)),
    );
  });

  test("automatically compacts above the configured threshold", () => {
    const largeReply = "A".repeat(900);
    const model = makeTestLanguageModel((request) =>
      isCompactionRequest(request)
        ? [{ type: "text" as const, text: "Automatic summary." }]
        : [{ type: "text" as const, text: largeReply }],
    );
    const runtime = makeRuntime(model);

    return Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* AgentService;
        const sessionId = SessionId.make("automatic-compaction");

        for (let index = 0; index < 4; index += 1) {
          yield* send(agent, sessionId, `large-${index}-${"B".repeat(900)}`);
        }

        const events = Array.from(
          yield* agent.sendMessage(sessionId, `trigger-${"C".repeat(900)}`).pipe(Stream.runCollect),
        );
        expect(events[0]?._tag).toBe("CompactionCompleted");
        const compacted = events.find(AgentStreamEvent.guards.CompactionCompleted);
        expect(compacted?.result.reason).toBe("threshold");

        const snapshot = yield* agent.getSession(sessionId);
        expect(snapshot.compactions).toHaveLength(1);
        expect(snapshot.stats.rawMessageCount).toBe(10);
        expect(snapshot.stats.modelMessageCount).toBeLessThan(snapshot.stats.rawMessageCount);
      }).pipe(Effect.provide(runtime)),
    );
  });

  test("moves the boundary rather than splitting a tool interaction", () => {
    const model = makeTestLanguageModel((request) => {
      if (isCompactionRequest(request)) {
        return [{ type: "text" as const, text: "Tool-safe summary." }];
      }
      const hasToolResult = request.prompt.content.some((message) => message.role === "tool");
      const asksRuntime = request.prompt.content.some(
        (message) =>
          message.role === "user" &&
          message.content.some((part) => part.type === "text" && part.text.includes("runtime")),
      );
      if (asksRuntime && !hasToolResult) {
        return [
          {
            type: "tool-call" as const,
            id: "boundary-call",
            name: "lookup_project_fact",
            params: { topic: "runtime" },
          },
        ];
      }
      return [{ type: "text" as const, text: "done" }];
    });
    const runtime = makeRuntime(model);

    return Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* AgentService;
        const sessionId = SessionId.make("tool-boundary");
        yield* send(agent, sessionId, "Look up the runtime.");
        yield* send(agent, sessionId, "A short follow-up.");
        yield* send(agent, sessionId, "A second short follow-up.");

        const unsafeAttempt = yield* agent.compactSession(sessionId);
        expect(unsafeAttempt.compacted).toBe(false);
        expect(unsafeAttempt.reason).toBe("insufficient-history");

        yield* send(agent, sessionId, "Another follow-up.");
        const safeAttempt = yield* agent.compactSession(sessionId);
        expect(safeAttempt.compacted).toBe(true);

        const snapshot = yield* agent.getSession(sessionId);
        const messages = chatMessages(snapshot.chat);
        const overlay = snapshot.compactions.at(-1);
        expect(overlay?.toMessageId).toBe(messages[3]?.id);
        const coveredParts = messages.slice(0, 4).flatMap((entry) => messageParts(entry.message));
        expect(coveredParts.some((part) => part.type === "tool-call")).toBe(true);
        expect(coveredParts.some((part) => part.type === "tool-result")).toBe(true);
      }).pipe(Effect.provide(runtime)),
    );
  });
});
