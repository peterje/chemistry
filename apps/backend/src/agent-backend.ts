import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientError from "effect/unstable/http/HttpClientError";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import {
  AgentPersistenceError,
  AgentRpcs,
  AgentSessionRpcs,
  ChatCatalogRpcs,
  SessionId,
} from "@chemistry/contracts/agent-protocol";
import { LOCAL_AGENT_BACKEND_PORT } from "@chemistry/contracts/local-development";
import AgentSession from "./agent-durable-object.ts";
import ChatCatalogObject from "./chat-catalog-durable-object.ts";
import { DEPLOYMENT_MARKER } from "./deployment-marker.ts";

const decodeSessionId = Schema.decodeUnknownResult(SessionId);
const catalogRetrySchedule = Schedule.exponential("100 millis").pipe(
  Schedule.jittered,
  Schedule.upTo({ times: 10 }),
);

/** Private typed RPC Worker routing RPC and WebSocket requests to one session Durable Object. */
export default class AgentBackend extends Cloudflare.RpcWorker<AgentBackend>()(
  "AgentBackend",
  {
    main: import.meta.url,
    workersDev: true,
    dev: {
      port: LOCAL_AGENT_BACKEND_PORT,
      strictPort: true,
    },
    schema: AgentRpcs,
  },
  Effect.gen(function* () {
    const sessions = yield* AgentSession;
    const catalogs = yield* ChatCatalogObject;

    const clientFor = Effect.fn("AgentBackend.clientFor")(function* (sessionId: SessionId) {
      const stub = sessions.getByName(sessionId);
      const httpClient = HttpClient.make((request) =>
        stub.fetch(HttpServerRequest.fromClientRequest(request)).pipe(
          Effect.map((response) => HttpServerResponse.toClientResponse(response, { request })),
          Effect.mapError(
            (cause) =>
              new HttpClientError.HttpClientError({
                reason: new HttpClientError.TransportError({ request, cause }),
              }),
          ),
        ),
      );
      const protocol = RpcClient.layerProtocolHttp({
        url: "https://agent-session.internal/rpc",
      }).pipe(
        Layer.provide(RpcSerialization.layerNdjson),
        Layer.provide(Layer.succeed(HttpClient.HttpClient, httpClient)),
      );
      return yield* RpcClient.make(AgentSessionRpcs).pipe(Effect.provide(protocol));
    });

    const catalogClient = Effect.fn("AgentBackend.catalogClient")(function* () {
      const stub = catalogs.getByName("global");
      const httpClient = HttpClient.make((request) =>
        stub.fetch(HttpServerRequest.fromClientRequest(request)).pipe(
          Effect.map((response) => HttpServerResponse.toClientResponse(response, { request })),
          Effect.mapError(
            (cause) =>
              new HttpClientError.HttpClientError({
                reason: new HttpClientError.TransportError({ request, cause }),
              }),
          ),
        ),
      );
      const protocol = RpcClient.layerProtocolHttp({
        url: "https://chat-catalog.internal/rpc",
      }).pipe(
        Layer.provide(RpcSerialization.layerNdjson),
        Layer.provide(Layer.succeed(HttpClient.HttpClient, httpClient)),
      );
      return yield* RpcClient.make(ChatCatalogRpcs).pipe(Effect.provide(protocol));
    });

    const handlers = AgentRpcs.toLayer({
      createChat: ({ sessionId }) =>
        catalogClient().pipe(
          Effect.flatMap((client) => client.createChat({ sessionId })),
          Effect.retry(catalogRetrySchedule),
          Effect.mapError(
            (cause) =>
              new AgentPersistenceError({
                operation: "chat-catalog:create",
                message: cause.message,
              }),
          ),
          Effect.scoped,
        ),
      listChats: () =>
        catalogClient().pipe(
          Effect.flatMap((client) => client.listChats({})),
          Effect.retry(catalogRetrySchedule),
          Effect.mapError(
            (cause) =>
              new AgentPersistenceError({
                operation: "chat-catalog:list",
                message: cause.message,
              }),
          ),
          Effect.scoped,
        ),
      getSession: ({ sessionId }) =>
        clientFor(sessionId).pipe(
          Effect.flatMap((client) => client.getSession({ sessionId })),
          Effect.catchTag("RpcClientError", Effect.die),
          Effect.scoped,
        ),
      getRuntime: ({ sessionId }) =>
        clientFor(sessionId).pipe(
          Effect.flatMap((client) => client.getRuntime({ sessionId })),
          Effect.catchTag("RpcClientError", Effect.die),
          Effect.scoped,
        ),
      updateContext: ({ sessionId, context }) =>
        clientFor(sessionId).pipe(
          Effect.flatMap((client) => client.updateContext({ sessionId, context })),
          Effect.catchTag("RpcClientError", Effect.die),
          Effect.scoped,
        ),
      sendMessage: ({ sessionId, prompt }) =>
        Stream.scoped(
          Stream.unwrap(
            clientFor(sessionId).pipe(
              Effect.map((client) => client.sendMessage({ sessionId, prompt })),
            ),
          ),
        ).pipe(Stream.catchTag("RpcClientError", Stream.die)),
      compactSession: ({ sessionId }) =>
        clientFor(sessionId).pipe(
          Effect.flatMap((client) => client.compactSession({ sessionId })),
          Effect.catchTag("RpcClientError", Effect.die),
          Effect.scoped,
        ),
    });

    return Effect.gen(function* () {
      const rpcHandler = yield* RpcServer.toHttpEffect(AgentRpcs).pipe(
        Effect.provide(Layer.mergeAll(handlers, RpcSerialization.layerNdjson)),
      );

      return Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const url = new URL(request.url, "https://agent-backend.invalid");
        if (url.pathname.endsWith("/deployment-marker")) {
          return HttpServerResponse.text(DEPLOYMENT_MARKER);
        }
        if (url.pathname.endsWith("/session-deployment-marker")) {
          const decoded = decodeSessionId(url.searchParams.get("sessionId"));
          if (Result.isFailure(decoded)) {
            return HttpServerResponse.text("Invalid session id", { status: 400 });
          }
          return yield* sessions.getByName(decoded.success).fetch(request);
        }
        if (url.pathname.endsWith("/ws")) {
          const decoded = decodeSessionId(url.searchParams.get("sessionId"));
          if (Result.isFailure(decoded)) {
            return HttpServerResponse.text("Invalid session id", { status: 400 });
          }
          return yield* sessions.getByName(decoded.success).fetch(request);
        }
        return yield* rpcHandler;
      }).pipe(Effect.annotateLogs({ deploymentMarker: DEPLOYMENT_MARKER }));
    });
  }),
) {}
