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

const messageParts = (message: Prompt.Message): ReadonlyArray<Prompt.Part> =>
  message.role === "system" ? [] : message.content;

const requiredToolCall = (index: number) => [
  {
    type: "tool-call" as const,
    id: `call-${index + 1}`,
    name: "lookup_project_fact",
    params: { topic: "runtime" },
  },
];

describe("typed tool calling", () => {
  test("executes the Toolkit handler and continues to a final answer", () => {
    const model = makeTestLanguageModel((_request, index) =>
      index === 0
        ? requiredToolCall(index)
        : [{ type: "text" as const, text: "The runtime fact was applied." }],
    );
    const runtime = makeRuntime(model);

    return Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* AgentService;
        const observedModel = yield* TestLanguageModel;
        const sessionId = SessionId.make("tool-success");
        const events = Array.from(
          yield* agent.sendMessage(sessionId, "Where does this agent run?").pipe(Stream.runCollect),
        );

        expect(events.map((event) => event._tag)).toEqual([
          "TurnStarted",
          "ToolCall",
          "ToolResult",
          "TextDelta",
          "TextDelta",
          "TurnCompleted",
        ]);
        const result = events.find(AgentStreamEvent.guards.ToolResult);
        expect(result?.isFailure).toBe(false);
        expect(result?.output).toEqual({
          topic: "runtime",
          fact: "The agent runs on Cloudflare Workers and a per-session Durable Object.",
        });

        const snapshot = yield* agent.getSession(sessionId);
        const messages = chatMessages(snapshot.chat);
        expect(messages.map((entry) => entry.message.role)).toEqual([
          "user",
          "assistant",
          "tool",
          "assistant",
        ]);
        const storedToolResult = messages
          .flatMap((entry) => messageParts(entry.message))
          .find((part) => part.type === "tool-result");
        expect(storedToolResult?.isFailure).toBe(false);

        const requests = yield* observedModel.requests();
        expect(requests).toHaveLength(2);
        expect(requests[1]?.prompt.content.some((message) => message.role === "tool")).toBe(true);
      }).pipe(Effect.provide(runtime)),
    );
  });

  test("fails with the typed loop-limit error after the configured bound", () => {
    const model = makeTestLanguageModel((_request, index) => requiredToolCall(index));
    const runtime = makeRuntime(model);

    return Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* AgentService;
        const observedModel = yield* TestLanguageModel;
        const sessionId = SessionId.make("tool-loop-limit");
        const error = yield* agent
          .sendMessage(sessionId, "Keep looking up the runtime forever.")
          .pipe(Stream.runDrain, Effect.flip);

        expect(error._tag).toBe("ToolLoopLimitExceeded");
        if (error._tag === "ToolLoopLimitExceeded") {
          expect(error.maxSteps).toBe(5);
        }
        expect(yield* observedModel.requests()).toHaveLength(5);

        const snapshot = yield* agent.getSession(sessionId);
        const messages = chatMessages(snapshot.chat);
        expect(messages).toHaveLength(11);
        expect(messages.at(-1)?.message.role).toBe("tool");
      }).pipe(Effect.provide(runtime)),
    );
  });
});
