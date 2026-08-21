import * as Cloudflare from "alchemy/Cloudflare";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { AgentRpcs } from "../shared/agent-protocol.ts";
import { AgentService } from "./agent-service.ts";
import { AgentServiceLive } from "./agent-service-live.ts";
import { DurableObjectSessionStore } from "./durable-object-session-store.ts";
import { MessageIdSourceLive } from "./message-id-source.ts";
import { DEFAULT_WORKERS_AI_MODEL } from "./model-config.ts";

/**
 * Per-session typed RPC Durable Object hosting the Effect-native agent runtime.
 */
export default class AgentSession extends Cloudflare.RpcDurableObject<AgentSession>()(
  "AgentSession",
  { schema: AgentRpcs },
  Effect.gen(function* () {
    const ai = yield* Cloudflare.Workers.AI();
    const modelName = yield* Config.string("WORKERS_AI_MODEL").pipe(
      Effect.orElseSucceed(() => DEFAULT_WORKERS_AI_MODEL),
    );
    const languageModel = ai.model({
      model: modelName,
      parameters: { temperature: 0.2, maxTokens: 1_024 },
    });
    const agentLayer = AgentServiceLive.pipe(
      Layer.provide(DurableObjectSessionStore),
      Layer.provide(MessageIdSourceLive),
      Layer.provide(languageModel),
    );

    return Effect.gen(function* () {
      const agent = yield* AgentService;
      const handlers = AgentRpcs.toLayer({
        getSession: ({ sessionId }) => agent.getSession(sessionId),
        updateContext: ({ sessionId, context }) => agent.updateContext(sessionId, context),
        sendMessage: ({ sessionId, prompt }) => agent.sendMessage(sessionId, prompt),
        compactSession: ({ sessionId }) => agent.compactSession(sessionId),
      });

      return RpcServer.toHttpEffect(AgentRpcs).pipe(
        Effect.provide(Layer.mergeAll(handlers, RpcSerialization.layerNdjson)),
      );
    }).pipe(Effect.provide(agentLayer));
  }).pipe(Effect.provide(Cloudflare.Workers.AIBinding)),
) {}
