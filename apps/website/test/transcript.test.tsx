import { expect, test } from "bun:test";
import { initialRuntimeSocketSnapshot } from "@chemistry/client-runtime/runtime-client-state";
import {
  AgentStreamEvent,
  DurableStreamEvent,
  MessageId,
  OperationId,
  StreamId,
  TranscriptMessage,
  TranscriptPart,
} from "@chemistry/contracts/agent-protocol";
import { renderToStaticMarkup } from "react-dom/server";
import { Transcript } from "../src/client/components/transcript.tsx";

const streamId = StreamId.make("stream-reload");
const operationId = OperationId.make("operation-reload");
const prompt = "hi";

const turnStarted = DurableStreamEvent.make({
  streamId,
  operationId,
  sequence: 0,
  producedAt: 1,
  event: AgentStreamEvent.cases.TurnStarted.make({
    userMessage: TranscriptMessage.make({
      id: MessageId.make("message-reload"),
      role: "user",
      parts: [TranscriptPart.cases.Text.make({ text: prompt })],
      createdAt: 1,
    }),
  }),
});

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
