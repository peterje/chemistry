import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { AgentStreamEvent } from "@chemistry/contracts/agent-protocol";
import { AgentService, defaultAgentConfiguration } from "./agent-service.ts";
import { RuntimeRequestSnapshot } from "./runtime-state.ts";
import { TurnExecutor } from "./turn-executor.ts";

/** Optional deterministic fault behavior owned by the backend composition root. */
export type TurnExecutorFaultMode = "normal" | "abort-after-first-delta";

/** Build the adapter Layer exposing model conversation as a durable turn executor. */
export const agentTurnExecutorLayer = (modelName: string, faultMode: TurnExecutorFaultMode) =>
  Layer.effect(
    TurnExecutor,
    Effect.gen(function* () {
      const agent = yield* AgentService;
      return TurnExecutor.of({
        captureRequest: (input) =>
          agent.getSession(input.sessionId).pipe(
            Effect.map((session) =>
              RuntimeRequestSnapshot.make({
                version: 1,
                provider: "cloudflare-workers-ai",
                model: modelName,
                toolStepLimit: defaultAgentConfiguration.maxToolSteps,
                contextStrategy: "durable-session-at-phase-start",
                context: session.context,
                historyMessageIds: session.messages.map((message) => message.id),
                compactionIds: session.compactions.map((compaction) => compaction.id),
                submittedAt: input.submittedAt,
              }),
            ),
          ),
        execute: (input) => {
          const events = agent.runTurn(input);
          return faultMode === "abort-after-first-delta"
            ? events.pipe(
                Stream.take(1),
                Stream.concat(
                  Stream.make(
                    AgentStreamEvent.cases.TextDelta.make({
                      delta: "Durable execution reached a persisted partial checkpoint. ",
                    }),
                  ),
                ),
                Stream.concat(Stream.never),
              )
            : events;
        },
        persistPartial: (input) => agent.persistPartial(input),
        hasAssistantMessage: (sessionId, assistantMessageId) =>
          agent.hasAssistantMessage(sessionId, assistantMessageId),
      });
    }),
  );
