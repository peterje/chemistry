import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import {
  AgentContext,
  AgentStreamEvent,
  SessionId,
  chatMessages,
} from "@chemistry/contracts/agent-protocol";
import type * as Prompt from "effect/unstable/ai/Prompt";
import { AgentService } from "@chemistry/agent-runtime/agent-service";
import { AgentServiceLive } from "@chemistry/agent-runtime/agent-service-live";
import {
  DeterministicMessageIds,
  InMemorySessionStore,
  TestLanguageModel,
  makeTestLanguageModel,
} from "./support/test-layers.ts";

const textResponse = (text: string) => [{ type: "text" as const, text }];

const makeRuntime = (modelLayer: ReturnType<typeof makeTestLanguageModel>) => {
  const dependencies = Layer.mergeAll(InMemorySessionStore, DeterministicMessageIds, modelLayer);
  const agent = AgentServiceLive.pipe(Layer.provide(dependencies));
  return Layer.merge(agent, modelLayer);
};

const messageText = (message: Prompt.Message | undefined): string => {
  if (message === undefined) return "";
  if (message.role === "system") return message.content;
  return message.content
    .filter((part) => part.type === "text")
    .map((part) => part.text)
    .join("");
};

describe("durable message sending and context", () => {
  test("persists transcript history and isolates sessions", () => {
    const model = makeTestLanguageModel((_request, index) => textResponse(`reply-${index + 1}`));
    const runtime = makeRuntime(model);

    return Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* AgentService;
        const alpha = SessionId.make("alpha");
        const beta = SessionId.make("beta");

        yield* agent.sendMessage(alpha, "hello alpha").pipe(Stream.runDrain);

        const reloadedAlpha = yield* agent.getSession(alpha);
        const untouchedBeta = yield* agent.getSession(beta);

        const alphaMessages = chatMessages(reloadedAlpha.chat);
        expect(alphaMessages).toHaveLength(2);
        expect(alphaMessages.map((entry) => entry.message.role)).toEqual(["user", "assistant"]);
        expect(messageText(alphaMessages[0]?.message)).toBe("hello alpha");
        expect(messageText(alphaMessages[1]?.message)).toBe("reply-1");
        expect(chatMessages(untouchedBeta.chat)).toHaveLength(0);
      }).pipe(Effect.provide(runtime)),
    );
  });

  test("persists editable context and injects it into model requests", () => {
    const model = makeTestLanguageModel((request) => {
      const first = request.prompt.content[0];
      const system = first?.role === "system" ? first.content : "";
      return textResponse(system.includes("ultramarine") ? "context-seen" : "context-missing");
    });
    const runtime = makeRuntime(model);

    return Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* AgentService;
        const observedModel = yield* TestLanguageModel;
        const sessionId = SessionId.make("context-demo");
        const context = AgentContext.make({
          systemPrompt: "Answer from durable memory.",
          memory: "The user's favorite color is ultramarine.",
        });

        const updated = yield* agent.updateContext(sessionId, context);
        expect(updated.context).toEqual(context);

        yield* agent.sendMessage(sessionId, "What is my favorite color?").pipe(Stream.runDrain);

        const snapshot = yield* agent.getSession(sessionId);
        expect(snapshot.context).toEqual(context);
        expect(messageText(chatMessages(snapshot.chat)[1]?.message)).toBe("context-seen");

        const requests = yield* observedModel.requests();
        expect(requests).toHaveLength(1);
        const system = requests[0]?.prompt.content[0];
        expect(system?.role).toBe("system");
        if (system?.role === "system") {
          expect(system.content).toContain("ultramarine");
        }
      }).pipe(Effect.provide(runtime)),
    );
  });

  test("fails hard when a tool-capable model stream has no semantic output", () => {
    const model = makeTestLanguageModel(() => []);
    const runtime = makeRuntime(model);

    return Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* AgentService;
        const observedModel = yield* TestLanguageModel;
        const sessionId = SessionId.make("empty-stream-failure");

        const error = yield* agent.sendMessage(sessionId, "hi").pipe(Stream.runDrain, Effect.flip);

        expect(error._tag).toBe("AgentInferenceError");
        if (error._tag === "AgentInferenceError") {
          expect(error.operation).toBe("empty-model-stream");
        }
        const snapshot = yield* agent.getSession(sessionId);
        expect(chatMessages(snapshot.chat).map((entry) => entry.message.role)).toEqual(["user"]);
        const requests = yield* observedModel.requests();
        expect(requests).toHaveLength(1);
        expect(requests[0]?.tools.length).toBeGreaterThan(0);
      }).pipe(Effect.provide(runtime)),
    );
  });

  test("streams ordered turn events and returns the stored completion", () => {
    const model = makeTestLanguageModel(() => textResponse("streamed answer"));
    const runtime = makeRuntime(model);

    return Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* AgentService;
        const sessionId = SessionId.make("stream-demo");
        const collected = yield* agent
          .sendMessage(sessionId, "stream this")
          .pipe(Stream.runCollect);
        const events = Array.from(collected);

        expect(events.map((event) => event._tag)).toEqual([
          "TurnStarted",
          "TextDelta",
          "TextDelta",
          "TurnCompleted",
        ]);
        const streamedText = events
          .filter(AgentStreamEvent.guards.TextDelta)
          .map((event) => event.delta)
          .join("");
        expect(streamedText).toBe("streamed answer");
        const completed = events.find(AgentStreamEvent.guards.TurnCompleted);
        expect(completed?.assistantMessage.role).toBe("assistant");
        expect(completed?.stats.rawMessageCount).toBe(2);
      }).pipe(Effect.provide(runtime)),
    );
  });
});
