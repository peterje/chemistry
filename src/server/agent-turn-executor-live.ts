import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { AgentStreamEvent } from "../shared/agent-protocol.ts";
import { AgentService, defaultAgentConfiguration } from "./agent-service.ts";
import { LIVE_CHAOS_ABORT_AFTER_FIRST_DELTA } from "./deployment-marker.ts";
import { RuntimeRequestSnapshot } from "./runtime-state.ts";
import { TurnExecutor } from "./turn-executor.ts";

/** Build the adapter Layer exposing model conversation as a durable turn executor. */
export const agentTurnExecutorLayer = (modelName: string) =>
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
          return LIVE_CHAOS_ABORT_AFTER_FIRST_DELTA
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
