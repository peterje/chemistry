import * as Cloudflare from "alchemy/Cloudflare";
import type { WebSocket as AlchemyWebSocket } from "alchemy/Cloudflare";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  ConnectionId,
  RuntimeClientFrame,
  RuntimeServerFrame,
  SessionId,
  StreamId,
  type DurableStreamEvent,
} from "../shared/agent-protocol.ts";
import {
  decodeRuntimeClientFrame,
  encodeRuntimeClientFrame,
  encodeRuntimeServerFrame,
} from "../shared/runtime-protocol.ts";
import { LIVE_CHAOS_ABORT_AFTER_FIRST_DELTA } from "./deployment-marker.ts";
import { DurableExecution } from "./durable-execution.ts";
import {
  acceptReplayBacklog,
  beginReplayHandoff,
  drainReplayPending,
  parkLiveEvent,
  type ReplayHandoff,
} from "./replay-handoff.ts";
import { RuntimeIdSource } from "./runtime-id-source.ts";

const MAX_PENDING_HANDOFF_EVENTS = 256;

/** Schema-versioned state persisted by Cloudflare beside a hibernatable socket. */
export const RuntimeSocketAttachment = Schema.Struct({
  version: Schema.Literal(1),
  connectionId: ConnectionId,
  sessionId: SessionId,
  probeId: Schema.NonEmptyString,
  probedStreamId: Schema.NullOr(StreamId),
  sentStreamId: Schema.NullOr(StreamId),
  sentSequence: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(-1)),
  acknowledgedStreamId: Schema.NullOr(StreamId),
  acknowledgedSequence: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(-1)),
});

/** Decoded hibernatable socket attachment. */
export interface RuntimeSocketAttachment extends Schema.Schema.Type<
  typeof RuntimeSocketAttachment
> {}

const decodeAttachment = (socket: AlchemyWebSocket): RuntimeSocketAttachment | undefined => {
  const decoded = Schema.decodeUnknownResult(RuntimeSocketAttachment)(
    socket.deserializeAttachment<unknown>(),
  );
  return Result.isSuccess(decoded) ? decoded.success : undefined;
};

const encodeAttachment = (attachment: RuntimeSocketAttachment): RuntimeSocketAttachment =>
  RuntimeSocketAttachment.make(attachment);

/** Advance a hibernatable attachment cursor only within its sent high-water mark. */
export const applyRuntimeStreamAck = (
  attachment: RuntimeSocketAttachment,
  streamId: StreamId,
  sequence: number,
): RuntimeSocketAttachment | undefined => {
  if (attachment.sentStreamId !== streamId || sequence > attachment.sentSequence) {
    return undefined;
  }
  return encodeAttachment({
    ...attachment,
    acknowledgedStreamId: streamId,
    acknowledgedSequence: Math.max(attachment.acknowledgedSequence, sequence),
  });
};

const runtimeErrorMessage = (error: { readonly message: string }): string => error.message;

const buildRuntimeWebSocketAdapter = Effect.fn("RuntimeWebSocketAdapter.make")(function* () {
  const state = yield* Cloudflare.DurableObjectState;
  const runtime = yield* DurableExecution;
  const ids = yield* RuntimeIdSource;
  const replayGates = new Map<string, ReplayHandoff>();
  const keepAliveRequest = yield* encodeRuntimeClientFrame(
    RuntimeClientFrame.cases.KeepAlive.make({}),
  ).pipe(Effect.orDie);
  const keepAliveResponse = yield* encodeRuntimeServerFrame(
    RuntimeServerFrame.cases.KeepAliveAck.make({}),
  ).pipe(Effect.orDie);
  yield* state.setWebSocketAutoResponse(
    new WebSocketRequestResponsePair(keepAliveRequest, keepAliveResponse),
  );

  const send = Effect.fn("RuntimeWebSocketAdapter.send")(function* (
    socket: AlchemyWebSocket,
    frame: RuntimeServerFrame,
  ) {
    yield* socket.send(yield* encodeRuntimeServerFrame(frame));
  });

  const protocolError = Effect.fn("RuntimeWebSocketAdapter.protocolError")(function* (
    socket: AlchemyWebSocket,
    code:
      | "invalid-frame"
      | "frame-too-large"
      | "stale-probe"
      | "stale-stream"
      | "queue-full"
      | "runtime-unavailable",
    message: string,
    recoverable: boolean,
  ) {
    yield* send(
      socket,
      RuntimeServerFrame.cases.ProtocolError.make({ code, message, recoverable }),
    );
    if (!recoverable) yield* socket.close(1008, code);
  });

  const sendDurableEvent = Effect.fn("RuntimeWebSocketAdapter.sendDurableEvent")(function* (
    socket: AlchemyWebSocket,
    durableEvent: DurableStreamEvent,
    replay: boolean,
  ) {
    yield* send(socket, RuntimeServerFrame.cases.StreamEvent.make({ durableEvent, replay }));
    const attachment = decodeAttachment(socket);
    if (attachment !== undefined) {
      socket.serializeAttachment(
        encodeAttachment({
          ...attachment,
          sentStreamId: durableEvent.streamId,
          sentSequence: durableEvent.sequence,
        }),
      );
    }
  });

  const broadcast = Effect.fn("RuntimeWebSocketAdapter.broadcast")(function* (
    sessionId: SessionId,
    frame: RuntimeServerFrame,
  ) {
    const sockets = yield* state.getWebSockets();
    yield* Effect.forEach(
      sockets,
      Effect.fn("RuntimeWebSocketAdapter.broadcastOne")(function* (socket) {
        const attachment = decodeAttachment(socket);
        if (attachment === undefined || attachment.sessionId !== sessionId) return;
        const gate = replayGates.get(attachment.connectionId);
        if (frame._tag === "StreamEvent") {
          if (gate !== undefined && gate.streamId === frame.durableEvent.streamId) {
            const parked = parkLiveEvent(gate, frame.durableEvent, MAX_PENDING_HANDOFF_EVENTS);
            if (parked === "overflow") {
              replayGates.delete(attachment.connectionId);
              yield* socket.close(1013, "replay-handoff-overflow");
            }
            return;
          }
        }
        if (frame._tag === "StreamTerminal" && gate?.streamId === frame.streamId) return;
        yield* send(socket, frame);
      }),
      { discard: true },
    );
  });

  const broadcastTerminal = Effect.fn("RuntimeWebSocketAdapter.broadcastTerminal")(function* (
    sessionId: SessionId,
    streamId: StreamId,
  ) {
    const terminal = yield* runtime.terminal(sessionId, streamId);
    if (terminal === null) return;
    yield* broadcast(
      sessionId,
      RuntimeServerFrame.cases.StreamTerminal.make({
        streamId: terminal.streamId,
        operationId: terminal.operationId,
        status: terminal.status,
        sequence: terminal.sequence,
        generation: terminal.generation,
        attempt: terminal.attempt,
        recoveryWork: terminal.recoveryWork,
        reason: terminal.reason,
      }),
    );
    yield* runtime.cleanup(sessionId);
    if (terminal.cleanupAt !== null) yield* state.storage.setAlarm(terminal.cleanupAt);
  });

  const broadcastEvent = Effect.fn("RuntimeWebSocketAdapter.broadcastEvent")(function* (
    sessionId: SessionId,
    durableEvent: DurableStreamEvent,
  ) {
    yield* broadcast(
      sessionId,
      RuntimeServerFrame.cases.StreamEvent.make({ durableEvent, replay: false }),
    );
    if (LIVE_CHAOS_ABORT_AFTER_FIRST_DELTA && durableEvent.event._tag === "TextDelta") {
      yield* state.storage.setAlarm((yield* Clock.currentTimeMillis) + 15_000);
    }
  });

  const runAccepted = Effect.fn("RuntimeWebSocketAdapter.runAccepted")(function* (
    sessionId: SessionId,
    operationId: Parameters<typeof runtime.run>[1],
    streamId: StreamId,
  ) {
    const outcome = yield* runtime.run(sessionId, operationId).pipe(
      Stream.runForEach((durableEvent) => broadcastEvent(sessionId, durableEvent)),
      Effect.result,
    );
    if (Result.isFailure(outcome)) {
      const sockets = yield* state.getWebSockets();
      yield* Effect.forEach(
        sockets,
        (socket) =>
          protocolError(socket, "runtime-unavailable", runtimeErrorMessage(outcome.failure), true),
        { discard: true },
      );
    }
    yield* broadcastTerminal(sessionId, streamId);
    const wake = yield* runtime.wake(sessionId);
    if (wake.recoveryAlarmAt !== null) {
      yield* state.storage.setAlarm(wake.recoveryAlarmAt);
    }
    if (wake.runnableOperationId !== null && wake.runnableStreamId !== null) {
      yield* state.storage.setAlarm((yield* Clock.currentTimeMillis) + 1);
    }
  });

  const handleResume = Effect.fn("RuntimeWebSocketAdapter.resume")(function* (
    socket: AlchemyWebSocket,
    attachment: RuntimeSocketAttachment,
    frame: Extract<RuntimeClientFrame, { readonly _tag: "ResumeAck" }>,
  ) {
    if (frame.probeId !== attachment.probeId) {
      yield* protocolError(
        socket,
        "stale-probe",
        "Resume ACK does not match the latest probe",
        true,
      );
      return;
    }
    const selectedStreamId = frame.streamId ?? attachment.probedStreamId;
    if (
      frame.streamId !== null &&
      attachment.probedStreamId !== null &&
      frame.streamId !== attachment.probedStreamId
    ) {
      yield* protocolError(socket, "stale-stream", "Resume ACK targets a stale stream", true);
      return;
    }
    if (selectedStreamId === null) {
      replayGates.delete(attachment.connectionId);
      yield* send(
        socket,
        RuntimeServerFrame.cases.ResumeComplete.make({
          streamId: null,
          throughSequence: -1,
        }),
      );
      return;
    }

    const existingGate = replayGates.get(attachment.connectionId);
    const gate =
      existingGate?.streamId === selectedStreamId
        ? existingGate
        : beginReplayHandoff(selectedStreamId, frame.afterSequence);
    gate.highWater = frame.afterSequence;
    replayGates.set(attachment.connectionId, gate);
    const replayResult = yield* runtime
      .replay(attachment.sessionId, selectedStreamId, frame.afterSequence)
      .pipe(Effect.result);
    if (Result.isFailure(replayResult)) {
      replayGates.delete(attachment.connectionId);
      if (replayResult.failure._tag === "RuntimeCursorError") {
        yield* protocolError(socket, "stale-stream", replayResult.failure.message, false);
        return;
      }
      return yield* replayResult.failure;
    }
    const replay = replayResult.success;
    for (const durableEvent of acceptReplayBacklog(gate, replay.events)) {
      yield* sendDurableEvent(socket, durableEvent, true);
    }

    while (gate.pending.length > 0) {
      for (const durableEvent of drainReplayPending(gate)) {
        yield* sendDurableEvent(socket, durableEvent, true);
      }
    }
    replayGates.delete(attachment.connectionId);
    socket.serializeAttachment(
      encodeAttachment({
        ...attachment,
        sentStreamId: selectedStreamId,
        sentSequence: gate.highWater,
      }),
    );
    yield* send(
      socket,
      RuntimeServerFrame.cases.ResumeComplete.make({
        streamId: selectedStreamId,
        throughSequence: gate.highWater,
      }),
    );
    const terminal = yield* runtime.terminal(attachment.sessionId, selectedStreamId);
    if (terminal !== null) {
      yield* send(
        socket,
        RuntimeServerFrame.cases.StreamTerminal.make({
          streamId: terminal.streamId,
          operationId: terminal.operationId,
          status: terminal.status,
          sequence: terminal.sequence,
          generation: terminal.generation,
          attempt: terminal.attempt,
          recoveryWork: terminal.recoveryWork,
          reason: terminal.reason,
        }),
      );
    }
  });

  const dispatch = Effect.fn("RuntimeWebSocketAdapter.dispatch")(function* (
    socket: AlchemyWebSocket,
    attachment: RuntimeSocketAttachment,
    frame: RuntimeClientFrame,
  ) {
    switch (frame._tag) {
      case "ResumeAck":
        yield* handleResume(socket, attachment, frame);
        return;
      case "SubmitTurn": {
        const admission = yield* runtime.admit(
          attachment.sessionId,
          frame.prompt,
          frame.submissionId,
        );
        yield* send(
          socket,
          RuntimeServerFrame.cases.TurnAccepted.make({
            submissionId: admission.operation.submissionId,
            operationId: admission.operation.operationId,
            streamId: admission.operation.streamId,
            queuePosition: admission.queuePosition,
          }),
        );
        if (!admission.duplicate) {
          yield* state.waitUntil(
            runAccepted(
              attachment.sessionId,
              admission.operation.operationId,
              admission.operation.streamId,
            ),
          );
        } else {
          const terminal = yield* runtime.terminal(
            attachment.sessionId,
            admission.operation.streamId,
          );
          if (terminal !== null) {
            yield* send(
              socket,
              RuntimeServerFrame.cases.StreamTerminal.make({
                streamId: terminal.streamId,
                operationId: terminal.operationId,
                status: terminal.status,
                sequence: terminal.sequence,
                generation: terminal.generation,
                attempt: terminal.attempt,
                recoveryWork: terminal.recoveryWork,
                reason: terminal.reason,
              }),
            );
          }
        }
        return;
      }
      case "StreamAck": {
        const acknowledged = applyRuntimeStreamAck(attachment, frame.streamId, frame.sequence);
        if (acknowledged === undefined) {
          yield* protocolError(
            socket,
            "stale-stream",
            "Stream ACK exceeds the events sent to this connection",
            true,
          );
          return;
        }
        socket.serializeAttachment(acknowledged);
        return;
      }
      case "KeepAlive":
        yield* send(socket, RuntimeServerFrame.cases.KeepAliveAck.make({}));
        return;
      case "Ping": {
        const probe = yield* runtime.probe(attachment.sessionId);
        yield* send(
          socket,
          RuntimeServerFrame.cases.Pong.make({
            nonce: frame.nonce,
            bootId: probe.snapshot.bootId,
          }),
        );
        return;
      }
    }
  });

  const upgrade = Effect.fn("RuntimeWebSocketAdapter.upgrade")(function* (sessionId: SessionId) {
    const wake = yield* runtime.wake(sessionId);
    if (wake.recoveryAlarmAt !== null) {
      yield* state.storage.setAlarm(wake.recoveryAlarmAt);
    }
    if (wake.runnableOperationId !== null && wake.runnableStreamId !== null) {
      yield* state.waitUntil(
        runAccepted(sessionId, wake.runnableOperationId, wake.runnableStreamId),
      );
    }
    const [response, socket] = yield* Cloudflare.upgrade();
    const connectionId = yield* ids.connection();
    const probeId = `probe-${connectionId}`;
    const probe = yield* runtime.probe(sessionId);
    const attachment = encodeAttachment({
      version: 1,
      connectionId,
      sessionId,
      probeId,
      probedStreamId: probe.streamId,
      sentStreamId: null,
      sentSequence: -1,
      acknowledgedStreamId: null,
      acknowledgedSequence: -1,
    });
    socket.serializeAttachment(attachment);
    if (probe.streamId !== null) {
      replayGates.set(connectionId, beginReplayHandoff(probe.streamId, -1));
    }
    yield* send(
      socket,
      RuntimeServerFrame.cases.ResumeProbe.make({
        probeId,
        connectionId,
        sessionId,
        activeStreamId: probe.streamId,
        latestSequence: probe.latestSequence,
        runtime: probe.snapshot,
      }),
    );
    return response;
  });

  const onMessage = Effect.fn("RuntimeWebSocketAdapter.onMessage")(function* (
    socket: AlchemyWebSocket,
    message: string | ArrayBuffer,
  ) {
    const attachment = decodeAttachment(socket);
    if (attachment === undefined) {
      yield* protocolError(socket, "invalid-frame", "Socket attachment is invalid", false);
      return;
    }
    const wake = yield* runtime.wake(attachment.sessionId);
    if (wake.recoveryAlarmAt !== null) yield* state.storage.setAlarm(wake.recoveryAlarmAt);
    if (wake.runnableOperationId !== null && wake.runnableStreamId !== null) {
      yield* state.waitUntil(
        runAccepted(attachment.sessionId, wake.runnableOperationId, wake.runnableStreamId),
      );
    }
    const encodedMessage = Schema.decodeUnknownResult(Schema.String)(message);
    if (Result.isFailure(encodedMessage)) {
      yield* protocolError(
        socket,
        "invalid-frame",
        "Binary runtime frames are not supported",
        false,
      );
      return;
    }
    const decoded = yield* decodeRuntimeClientFrame(encodedMessage.success).pipe(Effect.result);
    if (Result.isFailure(decoded)) {
      yield* protocolError(
        socket,
        decoded.failure.code === "frame-too-large" ? "frame-too-large" : "invalid-frame",
        decoded.failure.message,
        false,
      );
      return;
    }
    if (decoded.success._tag === "SubmitTurn" && wake.recoverableOperationId !== null) {
      yield* protocolError(
        socket,
        "runtime-unavailable",
        "The previous durable turn is recovering; retry after it terminalizes",
        true,
      );
      return;
    }
    const dispatched = yield* dispatch(socket, attachment, decoded.success).pipe(Effect.result);
    if (Result.isFailure(dispatched)) {
      const code =
        dispatched.failure._tag === "RuntimeCapacityError" ? "queue-full" : "runtime-unavailable";
      yield* protocolError(socket, code, runtimeErrorMessage(dispatched.failure), true);
    }
  });

  const onClose = Effect.fn("RuntimeWebSocketAdapter.onClose")((socket: AlchemyWebSocket) =>
    Effect.sync(() => {
      const attachment = decodeAttachment(socket);
      if (attachment !== undefined) replayGates.delete(attachment.connectionId);
    }),
  );

  const broadcastRecovering = Effect.fn("RuntimeWebSocketAdapter.broadcastRecovering")(function* (
    sessionId: SessionId,
    operation: Parameters<typeof RuntimeServerFrame.cases.Recovering.make>[0]["operation"],
  ) {
    yield* broadcast(sessionId, RuntimeServerFrame.cases.Recovering.make({ operation }));
  });

  const recoverSession = Effect.fn("RuntimeWebSocketAdapter.recoverSession")(function* (
    sessionId: SessionId,
  ) {
    const probe = yield* runtime.probe(sessionId);
    if (probe.snapshot.activeOperation !== null) {
      yield* broadcastRecovering(sessionId, probe.snapshot.activeOperation);
    }
    const outcome = yield* runtime.recover(sessionId).pipe(
      Stream.runForEach((durableEvent) => broadcastEvent(sessionId, durableEvent)),
      Effect.result,
    );
    if (Result.isFailure(outcome)) {
      const sockets = yield* state.getWebSockets();
      yield* Effect.forEach(
        sockets,
        (socket) =>
          protocolError(socket, "runtime-unavailable", runtimeErrorMessage(outcome.failure), true),
        { discard: true },
      );
    }
    if (probe.streamId !== null) yield* broadcastTerminal(sessionId, probe.streamId);
    const wake = yield* runtime.wake(sessionId);
    if (wake.recoveryAlarmAt !== null) {
      yield* state.storage.setAlarm(wake.recoveryAlarmAt);
    }
    return wake;
  });

  return {
    upgrade,
    onMessage,
    onClose,
    broadcastEvent,
    broadcastTerminal,
    broadcastRecovering,
    runAccepted,
    recoverSession,
  };
});

type RuntimeWebSocketOperations = Effect.Success<ReturnType<typeof buildRuntimeWebSocketAdapter>>;

/** Hibernatable WebSocket protocol adapter installed at the Durable Object root. */
export class RuntimeWebSocket extends Context.Service<
  RuntimeWebSocket,
  RuntimeWebSocketOperations
>()("@chemistry/RuntimeWebSocket") {}

/** Live hibernatable WebSocket protocol Layer. */
export const RuntimeWebSocketLive = Layer.effect(RuntimeWebSocket, buildRuntimeWebSocketAdapter());
