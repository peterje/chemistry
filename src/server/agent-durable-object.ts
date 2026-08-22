import * as Cloudflare from "alchemy/Cloudflare";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import {
  AgentInferenceError,
  AgentPersistenceError,
  AgentRpcs,
  SessionId,
  type AgentRpcError,
} from "../shared/agent-protocol.ts";
import { AgentService } from "./agent-service.ts";
import { AgentServiceLive } from "./agent-service-live.ts";
import { agentTurnExecutorLayer } from "./agent-turn-executor-live.ts";
import {
  DurableExecution,
  DurableExecutionLive,
  type DurableExecutionError,
} from "./durable-execution.ts";
import { DurableObjectRuntimeStore } from "./durable-object-runtime-store.ts";
import { DurableObjectSessionStore } from "./durable-object-session-store.ts";
import { LIVE_CHAOS_ABORT_AFTER_FIRST_DELTA } from "./deployment-marker.ts";
import { MessageIdSourceLive } from "./message-id-source.ts";
import { DEFAULT_WORKERS_AI_MODEL } from "./model-config.ts";
import { RuntimeIdSourceLive } from "./runtime-id-source.ts";
import { RuntimeWebSocket, RuntimeWebSocketLive } from "./runtime-websocket-adapter.ts";

const decodeSessionId = Schema.decodeUnknownResult(SessionId);

const runtimeRpcError = (error: DurableExecutionError): AgentRpcError => {
  switch (error._tag) {
    case "AgentPersistenceError":
    case "AgentInferenceError":
    case "AgentCompactionError":
    case "ToolLoopLimitExceeded":
      return error;
    case "RuntimePersistenceError":
      return new AgentPersistenceError({
        operation: error.operation,
        message: error.message,
      });
    case "RuntimeCapacityError":
    case "RuntimeCursorError":
    case "RuntimeFenceError":
    case "RuntimeTransitionError":
      return new AgentInferenceError({
        operation: "durable-execution",
        message: error.message,
      });
  }
};

const logBoundaryFailure = (operation: string, cause: unknown) =>
  Effect.logError(`${operation} failed`).pipe(
    Effect.annotateLogs({
      cause: cause instanceof Error ? cause.message : String(cause),
    }),
  );

/** Per-session Durable Object hosting Effect RPC, hibernatable sockets, and durable execution. */
export default class AgentSession extends Cloudflare.DurableObject<AgentSession>()(
  "AgentSession",
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const ai = yield* Cloudflare.Workers.AI();
    const modelName = DEFAULT_WORKERS_AI_MODEL;
    const languageModel = ai.model({
      model: modelName,
      parameters: { temperature: 0.2, maxTokens: 1_024 },
    });

    const agentLayer = AgentServiceLive.pipe(
      Layer.provide(DurableObjectSessionStore),
      Layer.provide(MessageIdSourceLive),
      Layer.provide(languageModel),
    );
    const turnExecutorLayer = agentTurnExecutorLayer(modelName).pipe(Layer.provide(agentLayer));
    const durableExecutionLayer = DurableExecutionLive.pipe(
      Layer.provide(DurableObjectRuntimeStore),
      Layer.provide(RuntimeIdSourceLive),
      Layer.provide(turnExecutorLayer),
    );
    const runtimeWebSocketLayer = RuntimeWebSocketLive.pipe(
      Layer.provide(durableExecutionLayer),
      Layer.provide(RuntimeIdSourceLive),
    );
    const applicationLayer = Layer.mergeAll(
      agentLayer,
      durableExecutionLayer,
      RuntimeIdSourceLive,
      runtimeWebSocketLayer,
    );

    return Effect.gen(function* () {
      const agent = yield* AgentService;
      const runtime = yield* DurableExecution;
      const sockets = yield* RuntimeWebSocket;

      const ready = Effect.fn("AgentSession.ready")(function* (sessionId: SessionId) {
        const wake = yield* runtime.wake(sessionId).pipe(Effect.mapError(runtimeRpcError));
        if (wake.recoveryAlarmAt !== null) yield* state.storage.setAlarm(wake.recoveryAlarmAt);
        if (wake.runnableOperationId !== null && wake.runnableStreamId !== null) {
          yield* state.waitUntil(
            sockets.runAccepted(sessionId, wake.runnableOperationId, wake.runnableStreamId),
          );
        }
        return wake;
      });

      const turnReady = Effect.fn("AgentSession.turnReady")(function* (sessionId: SessionId) {
        const wake = yield* ready(sessionId);
        if (wake.recoverableOperationId !== null) {
          return yield* new AgentInferenceError({
            operation: "durable-recovery-pending",
            message: "The previous durable turn must recover before session mutation",
          });
        }
        return wake;
      });

      const mutableSession = Effect.fn("AgentSession.mutableSession")(function* (
        sessionId: SessionId,
      ) {
        const wake = yield* turnReady(sessionId);
        if (wake.snapshot.activeOperation !== null) {
          return yield* new AgentInferenceError({
            operation: "durable-turn-active",
            message: "Context and compaction cannot change during an active durable turn",
          });
        }
      });

      const handlers = AgentRpcs.toLayer({
        getSession: ({ sessionId }) =>
          ready(sessionId).pipe(Effect.andThen(agent.getSession(sessionId))),
        getRuntime: ({ sessionId }) =>
          ready(sessionId).pipe(
            Effect.andThen(runtime.probe(sessionId)),
            Effect.map((probe) => probe.snapshot),
            Effect.mapError(runtimeRpcError),
          ),
        updateContext: ({ sessionId, context }) =>
          mutableSession(sessionId).pipe(Effect.andThen(agent.updateContext(sessionId, context))),
        sendMessage: ({ sessionId, prompt }) =>
          Stream.unwrap(
            turnReady(sessionId).pipe(
              Effect.andThen(runtime.admit(sessionId, prompt)),
              Effect.mapError(runtimeRpcError),
              Effect.map((admission) =>
                runtime.run(sessionId, admission.operation.operationId).pipe(
                  Stream.tap((durableEvent) =>
                    sockets
                      .broadcastEvent(sessionId, durableEvent)
                      .pipe(Effect.result, Effect.asVoid),
                  ),
                  Stream.map((durableEvent) => durableEvent.event),
                  Stream.mapError(runtimeRpcError),
                  Stream.ensuring(
                    Effect.gen(function* () {
                      yield* sockets
                        .broadcastTerminal(sessionId, admission.operation.streamId)
                        .pipe(Effect.result, Effect.asVoid);
                      const wake = yield* runtime.wake(sessionId);
                      if (wake.recoveryAlarmAt !== null) {
                        yield* state.storage.setAlarm(wake.recoveryAlarmAt);
                      }
                      if (wake.runnableOperationId !== null && wake.runnableStreamId !== null) {
                        yield* state.storage.setAlarm((yield* Clock.currentTimeMillis) + 1);
                      }
                    }).pipe(Effect.result, Effect.asVoid),
                  ),
                ),
              ),
            ),
          ),
        compactSession: ({ sessionId }) =>
          mutableSession(sessionId).pipe(Effect.andThen(agent.compactSession(sessionId))),
      });

      const rpcHandler = RpcServer.toHttpEffect(AgentRpcs).pipe(
        Effect.provide(Layer.mergeAll(handlers, RpcSerialization.layerNdjson)),
      );

      const fetch = Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const url = new URL(request.url, "https://agent-session.invalid");
        if (url.pathname.endsWith("/ws")) {
          const decoded = decodeSessionId(url.searchParams.get("sessionId"));
          if (Result.isFailure(decoded)) {
            return HttpServerResponse.text("Invalid session id", { status: 400 });
          }
          const upgraded = yield* sockets.upgrade(decoded.success).pipe(Effect.result);
          if (Result.isFailure(upgraded)) {
            yield* logBoundaryFailure("AgentSession.upgrade", upgraded.failure);
            return HttpServerResponse.text("WebSocket upgrade failed", { status: 500 });
          }
          return upgraded.success;
        }
        return yield* rpcHandler;
      });

      const webSocketMessage = Effect.fn("AgentSession.webSocketMessage")(function* (
        socket: Parameters<typeof sockets.onMessage>[0],
        message: string | ArrayBuffer,
      ) {
        const handled = yield* sockets.onMessage(socket, message).pipe(Effect.result);
        if (Result.isFailure(handled)) {
          yield* logBoundaryFailure("AgentSession.webSocketMessage", handled.failure);
        }
      });

      const webSocketClose = Effect.fn("AgentSession.webSocketClose")(function* (
        socket: Parameters<typeof sockets.onClose>[0],
      ) {
        yield* sockets.onClose(socket);
      });

      const alarm = Effect.fn("AgentSession.alarm")(function* () {
        if (LIVE_CHAOS_ABORT_AFTER_FIRST_DELTA) {
          yield* state.abort("credentialed-live-chaos-isolate-loss");
          return;
        }
        const decoded = decodeSessionId(state.id.name);
        if (Result.isFailure(decoded)) {
          yield* logBoundaryFailure(
            "AgentSession.alarm",
            "Named Durable Object identity is unavailable",
          );
          return;
        }
        const outcome = yield* Effect.gen(function* () {
          const wake = yield* sockets.recoverSession(decoded.success);
          if (wake.runnableOperationId !== null && wake.runnableStreamId !== null) {
            yield* state.waitUntil(
              sockets.runAccepted(decoded.success, wake.runnableOperationId, wake.runnableStreamId),
            );
          }
          yield* runtime.cleanup(decoded.success);
        }).pipe(Effect.result);
        if (Result.isFailure(outcome)) {
          yield* logBoundaryFailure("AgentSession.alarm", outcome.failure);
        }
      });

      return {
        fetch,
        alarm,
        webSocketMessage,
        webSocketClose,
      };
    }).pipe(Effect.provide(applicationLayer));
  }).pipe(Effect.provide(Cloudflare.Workers.AIBinding)),
) {}
