import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { AgentRpcs } from "../shared/agent-protocol.ts";
import AgentSession from "./agent-durable-object.ts";

/** Private typed RPC Worker routing each request to its session Durable Object. */
export default class AgentBackend extends Cloudflare.RpcWorker<AgentBackend>()(
  "AgentBackend",
  {
    main: import.meta.url,
    workersDev: false,
    schema: AgentRpcs,
  },
  Effect.gen(function* () {
    const sessions = yield* AgentSession;
    const clientFor = (sessionId: string) => sessions.getByName(sessionId);

    const handlers = AgentRpcs.toLayer({
      getSession: ({ sessionId }) =>
        Effect.flatMap(clientFor(sessionId), (client) => client.getSession({ sessionId })).pipe(
          Effect.catchTag("RpcClientError", Effect.die),
        ),
      updateContext: ({ sessionId, context }) =>
        Effect.flatMap(clientFor(sessionId), (client) =>
          client.updateContext({ sessionId, context }),
        ).pipe(Effect.catchTag("RpcClientError", Effect.die)),
      sendMessage: ({ sessionId, prompt }) =>
        clientFor(sessionId).pipe(
          Effect.map((client) => client.sendMessage({ sessionId, prompt })),
          Stream.unwrap,
          Stream.catchTag("RpcClientError", Stream.die),
        ),
      compactSession: ({ sessionId }) =>
        Effect.flatMap(clientFor(sessionId), (client) => client.compactSession({ sessionId })).pipe(
          Effect.catchTag("RpcClientError", Effect.die),
        ),
    });

    return RpcServer.toHttpEffect(AgentRpcs).pipe(
      Effect.provide(Layer.mergeAll(handlers, RpcSerialization.layerNdjson)),
    );
  }),
) {}
