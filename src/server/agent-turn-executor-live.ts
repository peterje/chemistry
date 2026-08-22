import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { AgentStreamEvent } from "../shared/agent-protocol.ts";
import { AgentService } from "./agent-service.ts";
import { LIVE_CHAOS_ABORT_AFTER_FIRST_DELTA } from "./deployment-marker.ts";
import { TurnExecutor } from "./turn-executor.ts";

/** Adapter Layer exposing the existing model conversation module as a durable turn executor. */
export const AgentTurnExecutorLive = Layer.effect(
  TurnExecutor,
  Effect.gen(function* () {
    const agent = yield* AgentService;
    return TurnExecutor.of({
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
