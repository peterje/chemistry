import { expect, test } from "bun:test";
import { initialRuntimeSocketSnapshot } from "@chemistry/client-runtime/runtime-client-state";
import {
  AgentStreamEvent,
  MessageId,
  OperationId,
  StreamId,
  StreamMessage,
  StreamMessagePart,
} from "@chemistry/contracts/agent-protocol";
import * as Prompt from "effect/unstable/ai/Prompt";
import { renderToStaticMarkup } from "react-dom/server";
import { Transcript } from "../src/client/components/transcript.tsx";
import { TranscriptRow } from "../src/client/components/transcript-row.tsx";
import { nextHeldLiveTurns } from "../src/client/components/live-turn.tsx";

const streamId = StreamId.make("stream-reload");
const operationId = OperationId.make("operation-reload");
const prompt = "hi";
const turnStarted = {
  streamId,
  operationId,
  sequence: 0,
  producedAt: 1,
  event: AgentStreamEvent.cases.TurnStarted.make({
    userMessage: StreamMessage.make({
      id: MessageId.make("message-reload"),
      role: "user",
      parts: [StreamMessagePart.cases.Text.make({ text: prompt })],
      createdAt: 1,
    }),
  }),
};

test("in-flight replay renders before the transcript query resolves", () => {
  const runtime = {
    ...initialRuntimeSocketSnapshot(),
    status: "replaying" as const,
    streamId,
    operationId,
    checkpoint: "streaming",
    recentEvents: [turnStarted],
  };
  const markup = renderToStaticMarkup(
    <Transcript snapshot={undefined} runtime={runtime} failed={false} onRetry={() => undefined} />,
  );

  expect(markup).toContain(prompt);
  expect(markup).toContain("live-turn");
  expect(markup).not.toContain("Loading conversation");
});

test("a completed live turn stays visible until the transcript snapshot catches up", () => {
  const runtime = {
    ...initialRuntimeSocketSnapshot(),
    status: "completed" as const,
    streamId,
    operationId,
    checkpoint: "terminal" as const,
    recentEvents: [turnStarted],
  };
  const markup = renderToStaticMarkup(
    <Transcript snapshot={undefined} runtime={runtime} failed={false} onRetry={() => undefined} />,
  );

  expect(markup).toContain(prompt);
  expect(markup).toContain("live-turn");
});

test("a completed live turn stays held across a follow-up TurnAccepted reset", () => {
  const completed = {
    ...initialRuntimeSocketSnapshot(),
    status: "completed" as const,
    streamId,
    operationId,
    checkpoint: "terminal" as const,
    recentEvents: [
      turnStarted,
      {
        streamId,
        operationId,
        sequence: 1,
        producedAt: 2,
        event: AgentStreamEvent.cases.TextDelta.make({ delta: "hello" }),
      },
    ],
  };
  const held = nextHeldLiveTurns([], completed, new Set());
  expect(held).toHaveLength(1);
  expect(held[0]?.prompt).toBe(prompt);
  expect(held[0]?.text).toBe("hello");

  const nextTurn = {
    ...initialRuntimeSocketSnapshot(),
    status: "live" as const,
    checkpoint: "admitted" as const,
    recentEvents: [],
  };
  const stillHeld = nextHeldLiveTurns(held, nextTurn, new Set());
  expect(stillHeld).toHaveLength(1);
  expect(stillHeld[0]?.prompt).toBe(prompt);

  const caughtUp = nextHeldLiveTurns(stillHeld, nextTurn, new Set(["message-reload"]));
  expect(caughtUp).toHaveLength(0);
});

test("persisted reasoning parts are not rendered in the transcript", () => {
  const markup = renderToStaticMarkup(
    <TranscriptRow
      message={{
        id: MessageId.make("message-reasoning"),
        createdAt: 1,
        message: Prompt.assistantMessage({
          content: [
            Prompt.reasoningPart({ text: "hidden chain" }),
            Prompt.textPart({ text: "visible answer" }),
          ],
        }),
      }}
    />,
  );
  expect(markup).toContain("visible answer");
  expect(markup).not.toContain("hidden chain");
  expect(markup).not.toContain("Reasoning");
});

test("reasoning-only assistant messages are omitted", () => {
  const markup = renderToStaticMarkup(
    <TranscriptRow
      message={{
        id: MessageId.make("message-reasoning-only"),
        createdAt: 1,
        message: Prompt.assistantMessage({
          content: [Prompt.reasoningPart({ text: "hidden chain" })],
        }),
      }}
    />,
  );
  expect(markup).toBe("");
});
