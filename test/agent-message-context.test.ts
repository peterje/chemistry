import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import {
  AgentContext,
  AgentStreamEvent,
  SessionId,
  TranscriptPart,
} from "../src/shared/agent-protocol.ts";
import { AgentService } from "../src/server/agent-service.ts";
import { AgentServiceLive } from "../src/server/agent-service-live.ts";
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

const messageText = (parts: ReadonlyArray<TranscriptPart>): string =>
  parts
    .filter(TranscriptPart.guards.Text)
    .map((part) => part.text)
    .join("");

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

        expect(reloadedAlpha.messages).toHaveLength(2);
        expect(reloadedAlpha.messages.map((message) => message.role)).toEqual([
          "user",
          "assistant",
        ]);
        expect(messageText(reloadedAlpha.messages[0]?.parts ?? [])).toBe("hello alpha");
        expect(messageText(reloadedAlpha.messages[1]?.parts ?? [])).toBe("reply-1");
        expect(untouchedBeta.messages).toHaveLength(0);
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
        expect(messageText(snapshot.messages[1]?.parts ?? [])).toBe("context-seen");

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
