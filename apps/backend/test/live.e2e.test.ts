import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Test from "alchemy/Test/Bun";
import { expect, test as bunTest } from "bun:test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import Stack from "../../../alchemy.run.ts";
import {
  AgentContext,
  AgentRpcs,
  AgentStreamEvent,
  RuntimeClientFrame,
  RuntimeServerFrame,
  SessionId,
  SubmissionId,
  type RuntimeServerFrame as RuntimeServerFrameType,
  type SessionId as SessionIdType,
} from "@chemistry/contracts/agent-protocol";
import {
  decodeRuntimeServerFrame,
  encodeRuntimeClientFrame,
} from "@chemistry/contracts/runtime-protocol";

class LiveSocketFailure extends Schema.TaggedError<LiveSocketFailure>()("LiveSocketFailure", {
  message: Schema.String,
}) {}

type LiveInboxItem =
  | { readonly _tag: "Frame"; readonly frame: RuntimeServerFrameType }
  | { readonly _tag: "Failure"; readonly message: string };

interface LiveRuntimeSocket {
  readonly socket: WebSocket;
  readonly inbox: Queue.Queue<LiveInboxItem>;
}

interface RuntimeHandshake {
  readonly connection: LiveRuntimeSocket;
  readonly probe: Extract<RuntimeServerFrameType, { readonly _tag: "ResumeProbe" }>;
  readonly replayFrames: ReadonlyArray<RuntimeServerFrameType>;
}

const runtimeUrl = (websiteUrl: string, sessionId: SessionIdType): string => {
  const url = new URL("/ws", websiteUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("sessionId", sessionId);
  return url.toString();
};

const connectRuntime = Effect.fn("Live.connectRuntime")(function* (url: string) {
  const inbox = yield* Queue.unbounded<LiveInboxItem>();
  const socket = new WebSocket(url);
  socket.addEventListener("close", (event) => {
    Effect.runFork(
      Queue.offer(inbox, {
        _tag: "Failure",
        message: `Runtime socket closed (${event.code}): ${event.reason || "no reason"}`,
      }),
    );
  });
  socket.addEventListener("message", (event) => {
    const encoded = Schema.decodeUnknownResult(Schema.String)(event.data);
    if (Result.isFailure(encoded)) {
      Effect.runFork(
        Queue.offer(inbox, { _tag: "Failure", message: "Server sent a binary frame" }),
      );
      return;
    }
    Effect.runFork(
      decodeRuntimeServerFrame(encoded.success).pipe(
        Effect.matchEffect({
          onFailure: (error) => Queue.offer(inbox, { _tag: "Failure", message: error.message }),
          onSuccess: (frame) => Queue.offer(inbox, { _tag: "Frame", frame }),
        }),
      ),
    );
  });
  yield* Effect.callback<void, LiveSocketFailure>((resume) => {
    const onOpen = () => resume(Effect.void);
    const onError = () =>
      resume(
        Effect.fail(new LiveSocketFailure({ message: `Could not open runtime socket at ${url}` })),
      );
    socket.addEventListener("open", onOpen, { once: true });
    socket.addEventListener("error", onError, { once: true });
    return Effect.sync(() => {
      socket.removeEventListener("open", onOpen);
      socket.removeEventListener("error", onError);
    });
  });
  return { socket, inbox } satisfies LiveRuntimeSocket;
});

const nextRuntimeFrame = Effect.fn("Live.nextRuntimeFrame")(function* (
  connection: LiveRuntimeSocket,
) {
  const item = yield* Queue.take(connection.inbox).pipe(
    Effect.timeout("90 seconds"),
    Effect.mapError(
      () => new LiveSocketFailure({ message: "Timed out waiting for runtime frame" }),
    ),
  );
  if (item._tag === "Failure") return yield* new LiveSocketFailure({ message: item.message });
  if (item.frame._tag === "StreamEvent") {
    const acknowledgement = yield* encodeRuntimeClientFrame(
      RuntimeClientFrame.cases.StreamAck.make({
        streamId: item.frame.durableEvent.streamId,
        sequence: item.frame.durableEvent.sequence,
      }),
    );
    yield* Effect.sync(() => connection.socket.send(acknowledgement));
  }
  return item.frame;
});

const sendRuntimeFrame = Effect.fn("Live.sendRuntimeFrame")(function* (
  connection: LiveRuntimeSocket,
  frame: typeof RuntimeClientFrame.Type,
) {
  const encoded = yield* encodeRuntimeClientFrame(frame);
  yield* Effect.sync(() => connection.socket.send(encoded));
});

const closeRuntime = (connection: LiveRuntimeSocket): Effect.Effect<void> =>
  Effect.sync(() => connection.socket.close(1000, "live-test-boundary"));

const resumeRuntime = Effect.fn("Live.resumeRuntime")(function* (
  websiteUrl: string,
  sessionId: SessionIdType,
  afterSequence: number,
) {
  const connection = yield* connectRuntime(runtimeUrl(websiteUrl, sessionId));
  const first = yield* nextRuntimeFrame(connection);
  if (first._tag !== "ResumeProbe") {
    return yield* new LiveSocketFailure({
      message: `Expected ResumeProbe, received ${first._tag}`,
    });
  }
  yield* sendRuntimeFrame(
    connection,
    RuntimeClientFrame.cases.ResumeAck.make({
      probeId: first.probeId,
      streamId: first.activeStreamId,
      afterSequence,
    }),
  );
  const replayFrames: Array<RuntimeServerFrameType> = [];
  for (let index = 0; index < 2_048; index += 1) {
    const frame = yield* nextRuntimeFrame(connection);
    if (frame._tag === "ProtocolError") {
      return yield* new LiveSocketFailure({ message: `${frame.code}: ${frame.message}` });
    }
    if (frame._tag === "ResumeComplete") {
      return { connection, probe: first, replayFrames } satisfies RuntimeHandshake;
    }
    replayFrames.push(frame);
  }
  return yield* new LiveSocketFailure({ message: "Resume frame budget exhausted" });
});

const resumeEventually = Effect.fn("Live.resumeEventually")(function* (
  websiteUrl: string,
  sessionId: SessionIdType,
  afterSequence: number,
) {
  let lastMessage = "Runtime socket was unavailable";
  for (let attempt = 0; attempt < 12; attempt += 1) {
    const connected = yield* resumeRuntime(websiteUrl, sessionId, afterSequence).pipe(
      Effect.result,
    );
    if (Result.isSuccess(connected)) return connected.success;
    lastMessage = connected.failure.message;
    yield* Effect.sleep(`${Math.min(500 * 2 ** attempt, 8_000)} millis`);
  }
  return yield* new LiveSocketFailure({ message: lastMessage });
});

const collectThroughTerminal = Effect.fn("Live.collectThroughTerminal")(function* (
  connection: LiveRuntimeSocket,
  seed: ReadonlyArray<RuntimeServerFrameType>,
) {
  const frames = [...seed];
  const seededTerminal = frames.find(
    (frame) => frame._tag === "StreamTerminal" && frame.status !== "interrupted",
  );
  if (seededTerminal?._tag === "StreamTerminal") {
    return { frames, terminal: seededTerminal };
  }
  for (let index = 0; index < 4_096; index += 1) {
    const frame = yield* nextRuntimeFrame(connection);
    frames.push(frame);
    if (frame._tag === "ProtocolError" && !frame.recoverable) {
      return yield* new LiveSocketFailure({ message: `${frame.code}: ${frame.message}` });
    }
    if (frame._tag === "StreamTerminal" && frame.status !== "interrupted") {
      return { frames, terminal: frame };
    }
  }
  return yield* new LiveSocketFailure({ message: "Terminal frame budget exhausted" });
});

const streamSequences = (frames: ReadonlyArray<RuntimeServerFrameType>): ReadonlyArray<number> =>
  frames.filter(RuntimeServerFrame.guards.StreamEvent).map((frame) => frame.durableEvent.sequence);

const waitForDeploymentMarker = Effect.fn("Live.waitForDeploymentMarker")(function* (
  backendUrl: string,
  expected: string,
  sessionId?: SessionIdType,
) {
  const url = new URL(
    sessionId === undefined ? "/deployment-marker" : "/session-deployment-marker",
    backendUrl,
  );
  if (sessionId !== undefined) url.searchParams.set("sessionId", sessionId);
  for (let attempt = 0; attempt < 30; attempt += 1) {
    const observed = yield* Effect.tryPromise({
      try: async () => {
        const response = await fetch(url);
        return response.ok ? await response.text() : "";
      },
      catch: (cause) =>
        new LiveSocketFailure({
          message: cause instanceof Error ? cause.message : "Deployment marker request failed",
        }),
    });
    if (observed === expected) return;
    yield* Effect.sleep("1 second");
  }
  return yield* new LiveSocketFailure({
    message: `Deployment marker ${expected} did not become active`,
  });
});

const live = process.env.RUN_LIVE_E2E === "1";

if (!live) {
  bunTest.skip("live Cloudflare E2E (set RUN_LIVE_E2E=1 or run bun run test:e2e)", () => {});
} else {
  const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
    dev: false,
  });

  const stack = beforeAll(deploy(Stack));
  afterAll(destroy(Stack));

  const protocol = stack.pipe(
    Effect.map(({ websiteUrl }) =>
      Test.rpcClientLayer(new URL("/rpc", websiteUrl).toString(), {
        times: 10,
      }),
    ),
    Layer.unwrap,
  );

  test(
    "verifies messages, context, tools, compaction, and runtime snapshots through Effect RPC",
    Effect.gen(function* () {
      const { websiteUrl } = yield* stack;
      const ready = yield* Test.getWhenReady(websiteUrl);
      expect(ready.status).toBe(200);
      const client = yield* RpcClient.make(AgentRpcs);
      const sessionId = SessionId.make(`live-rpc-${crypto.randomUUID()}`);
      const verificationCode = `ORCHID-${crypto.randomUUID().slice(0, 8)}`;

      const createdChat = yield* client.createChat({ sessionId });
      expect(createdChat.title).toBe("New chat");
      expect((yield* client.listChats({})).chats.some((chat) => chat.sessionId === sessionId)).toBe(
        true,
      );

      const context = AgentContext.make({
        systemPrompt:
          "You are a concise verification assistant. Follow direct instructions exactly.",
        memory: `The verification code is ${verificationCode}.`,
      });
      const contextSnapshot = yield* client.updateContext({ sessionId, context });
      expect(contextSnapshot.context).toEqual(context);
      expect((yield* client.getRuntime({ sessionId })).queueDepth).toBe(0);

      let contextAnswer = "";
      for (
        let attempt = 0;
        attempt < 3 && !contextAnswer.includes(verificationCode);
        attempt += 1
      ) {
        const contextEvents = Array.from(
          yield* client
            .sendMessage({
              sessionId,
              prompt: "Reply with only the verification code from memory.",
            })
            .pipe(Stream.runCollect),
        );
        contextAnswer = contextEvents
          .filter(AgentStreamEvent.guards.TextDelta)
          .map((event) => event.delta)
          .join("");
      }
      expect(contextAnswer).toContain(verificationCode);
      const catalogEntry = (yield* client.listChats({})).chats.find(
        (chat) => chat.sessionId === sessionId,
      );
      expect(catalogEntry?.title).toBe("Reply with only the verification code from memory.");

      const toolEvents = Array.from(
        yield* client
          .sendMessage({
            sessionId,
            prompt: "Call lookup_project_fact with topic protocol, then answer using its result.",
          })
          .pipe(Stream.runCollect),
      );
      expect(toolEvents.some(AgentStreamEvent.guards.ToolCall)).toBe(true);
      const toolResult = toolEvents.find(AgentStreamEvent.guards.ToolResult);
      expect(toolResult?.isFailure).toBe(false);
      expect(JSON.stringify(toolResult?.output)).toContain("Effect RpcGroup");

      yield* client
        .sendMessage({ sessionId, prompt: "Reply with: acknowledged" })
        .pipe(Stream.runDrain);
      const before = yield* client.getSession({ sessionId });
      expect(before.messages.length).toBeGreaterThanOrEqual(8);
      const compaction = yield* client.compactSession({ sessionId });
      expect(compaction.compacted).toBe(true);
      const after = yield* client.getSession({ sessionId });
      expect(after.messages).toEqual(before.messages);
      expect(after.compactions.length).toBeGreaterThan(0);
      expect(after.stats.modelMessageCount).toBeLessThan(after.stats.rawMessageCount);
    }).pipe(Effect.provide(protocol)),
    { timeout: 420_000 },
  );

  test(
    "proves typed routing, reconnect replay, hibernation attachment wake, and RPC controls",
    Effect.gen(function* () {
      const { backendUrl, websiteUrl } = yield* stack;
      const client = yield* RpcClient.make(AgentRpcs);
      const sessionId = SessionId.make(`live-ws-${crypto.randomUUID()}`);
      const initial = yield* resumeEventually(websiteUrl, sessionId, -1);
      expect(initial.probe.sessionId).toBe(sessionId);
      expect(initial.probe.activeStreamId).toBeNull();

      const submissionId = SubmissionId.make(`submission-${crypto.randomUUID()}`);
      yield* sendRuntimeFrame(
        initial.connection,
        RuntimeClientFrame.cases.SubmitTurn.make({
          submissionId,
          prompt:
            "Explain durable replay in exactly three short sentences. Use plain text and no tools.",
        }),
      );
      let accepted: Extract<RuntimeServerFrameType, { readonly _tag: "TurnAccepted" }> | undefined;
      let firstEvent: Extract<RuntimeServerFrameType, { readonly _tag: "StreamEvent" }> | undefined;
      for (
        let index = 0;
        index < 128 && (accepted === undefined || firstEvent === undefined);
        index += 1
      ) {
        const frame = yield* nextRuntimeFrame(initial.connection);
        if (frame._tag === "TurnAccepted") accepted = frame;
        if (frame._tag === "StreamEvent") firstEvent = frame;
      }
      if (accepted === undefined || firstEvent === undefined) {
        return yield* new LiveSocketFailure({ message: "Turn was not accepted and started" });
      }
      expect(firstEvent.durableEvent.sequence).toBe(0);
      yield* closeRuntime(initial.connection);

      const resumed = yield* resumeEventually(
        websiteUrl,
        sessionId,
        firstEvent.durableEvent.sequence,
      );
      expect(resumed.probe.activeStreamId).toBe(accepted.streamId);
      const resumedResult = yield* collectThroughTerminal(resumed.connection, resumed.replayFrames);
      expect(resumedResult.terminal.status).toBe("completed");
      yield* Effect.logInfo("Reconnect replay terminal evidence", {
        sequence: resumedResult.terminal.sequence,
        generation: resumedResult.terminal.generation,
      });
      const combinedSequences = [
        firstEvent.durableEvent.sequence,
        ...streamSequences(resumedResult.frames),
      ];
      expect(combinedSequences).toEqual(
        Array.from({ length: resumedResult.terminal.sequence + 1 }, (_, index) => index),
      );

      yield* closeRuntime(resumed.connection);
      const fullReplay = yield* resumeEventually(backendUrl, sessionId, -1);
      const fullReplayResult = yield* collectThroughTerminal(
        fullReplay.connection,
        fullReplay.replayFrames,
      );
      expect(streamSequences(fullReplayResult.frames)).toEqual(combinedSequences);
      expect(fullReplayResult.terminal.operationId).toBe(accepted.operationId);
      yield* closeRuntime(fullReplay.connection);

      const hibernation = yield* resumeEventually(
        backendUrl,
        sessionId,
        fullReplayResult.terminal.sequence,
      );
      expect(streamSequences(hibernation.replayFrames)).toEqual([]);
      const hibernationReady = yield* collectThroughTerminal(
        hibernation.connection,
        hibernation.replayFrames,
      );
      expect(hibernationReady.terminal.operationId).toBe(accepted.operationId);
      for (let keepAlive = 0; keepAlive < 3; keepAlive += 1) {
        yield* Effect.sleep("20 seconds");
        yield* sendRuntimeFrame(
          hibernation.connection,
          RuntimeClientFrame.cases.KeepAlive.make({}),
        );
        expect((yield* nextRuntimeFrame(hibernation.connection))._tag).toBe("KeepAliveAck");
        yield* Effect.logInfo("Hibernation auto-response evidence", { keepAlive });
      }
      yield* Effect.sleep("15 seconds");
      const hibernationNonce = `hibernate-${crypto.randomUUID()}`;
      yield* sendRuntimeFrame(
        hibernation.connection,
        RuntimeClientFrame.cases.Ping.make({ nonce: hibernationNonce }),
      );
      let hibernatedBootId = hibernation.probe.runtime.bootId;
      for (let frameIndex = 0; frameIndex < 32; frameIndex += 1) {
        const frame = yield* nextRuntimeFrame(hibernation.connection);
        if (frame._tag === "Pong" && frame.nonce === hibernationNonce) {
          hibernatedBootId = frame.bootId;
          break;
        }
      }
      expect(hibernatedBootId).not.toBe(hibernation.probe.runtime.bootId);
      yield* Effect.logInfo("Hibernation wake evidence", {
        beforeBootId: hibernation.probe.runtime.bootId,
        afterBootId: hibernatedBootId,
      });

      const invalid = yield* resumeEventually(
        backendUrl,
        SessionId.make(`live-invalid-${crypto.randomUUID()}`),
        -1,
      );
      invalid.connection.socket.send("{not-json");
      const protocolFailure = yield* nextRuntimeFrame(invalid.connection);
      expect(protocolFailure._tag).toBe("ProtocolError");
      if (protocolFailure._tag === "ProtocolError") {
        expect(protocolFailure.code).toBe("invalid-frame");
        expect(protocolFailure.recoverable).toBe(false);
      }

      const staleCursor = yield* connectRuntime(runtimeUrl(backendUrl, sessionId));
      const staleProbe = yield* nextRuntimeFrame(staleCursor);
      if (staleProbe._tag !== "ResumeProbe") {
        return yield* new LiveSocketFailure({ message: "No resume probe for stale cursor test" });
      }
      yield* sendRuntimeFrame(
        staleCursor,
        RuntimeClientFrame.cases.ResumeAck.make({
          probeId: staleProbe.probeId,
          streamId: accepted.streamId,
          afterSequence: staleProbe.latestSequence + 1,
        }),
      );
      const staleFailure = yield* nextRuntimeFrame(staleCursor);
      expect(staleFailure._tag).toBe("ProtocolError");
      if (staleFailure._tag === "ProtocolError") {
        expect(staleFailure.code).toBe("stale-stream");
        expect(staleFailure.recoverable).toBe(false);
      }
      yield* closeRuntime(staleCursor);

      const otherSession = SessionId.make(`live-route-${crypto.randomUUID()}`);
      const routed = yield* resumeEventually(backendUrl, otherSession, -1);
      expect(routed.probe.sessionId).toBe(otherSession);
      expect(routed.probe.connectionId).not.toBe(hibernation.probe.connectionId);

      const updatedContext = AgentContext.make({
        systemPrompt: "You are a runtime verification assistant.",
        memory: "RPC controls remain available beside WebSockets.",
      });
      expect((yield* client.updateContext({ sessionId, context: updatedContext })).context).toEqual(
        updatedContext,
      );
      expect((yield* client.getSession({ sessionId })).messages.length).toBe(2);
      expect((yield* client.getRuntime({ sessionId })).activeOperation).toBeNull();
      yield* closeRuntime(hibernation.connection);
      yield* closeRuntime(invalid.connection);
      yield* closeRuntime(routed.connection);
    }).pipe(Effect.provide(protocol)),
    { timeout: 720_000 },
  );

  test(
    "forces a mid-turn Worker redeploy and converges through transcript continuation",
    Effect.gen(function* () {
      const { backendUrl, websiteUrl } = yield* stack;
      const client = yield* RpcClient.make(AgentRpcs);
      const markerPath = new URL("../src/deployment-marker.ts", import.meta.url).pathname;
      const originalMarker = yield* Effect.promise(() => Bun.file(markerPath).text());
      const sessionId = SessionId.make(`live-redeploy-${crypto.randomUUID()}`);
      const faultMarkerValue = `fault-${crypto.randomUUID()}`;
      const faultMarker = `/** Build marker changed only by the credentialed redeploy recovery test. */\nexport const DEPLOYMENT_MARKER = "${faultMarkerValue}";\n\n/** Compile-time fault gate used only by the credentialed isolate-loss test deployment. */\nexport const LIVE_CHAOS_ABORT_AFTER_FIRST_DELTA = true;\n`;
      const faultDeployment = yield* Effect.gen(function* () {
        yield* Effect.promise(() => Bun.write(markerPath, faultMarker));
        return yield* deploy(Stack);
      }).pipe(
        Effect.ensuring(
          Effect.promise(() => Bun.write(markerPath, originalMarker)).pipe(Effect.asVoid),
        ),
      );
      expect(faultDeployment.websiteUrl).toBe(websiteUrl);
      expect(faultDeployment.backendUrl).toBe(backendUrl);
      yield* waitForDeploymentMarker(backendUrl, faultMarkerValue);
      yield* waitForDeploymentMarker(backendUrl, faultMarkerValue, sessionId);

      const initial = yield* resumeEventually(backendUrl, sessionId, -1);
      yield* sendRuntimeFrame(
        initial.connection,
        RuntimeClientFrame.cases.SubmitTurn.make({
          submissionId: SubmissionId.make(`submission-${crypto.randomUUID()}`),
          prompt:
            "Explain durable execution in exactly eight short bullet points. Do not use tools.",
        }),
      );

      let accepted: Extract<RuntimeServerFrameType, { readonly _tag: "TurnAccepted" }> | undefined;
      const observed: Array<RuntimeServerFrameType> = [];
      let partialSequence = -1;
      for (let index = 0; index < 512 && partialSequence < 1; index += 1) {
        const frame = yield* nextRuntimeFrame(initial.connection);
        observed.push(frame);
        if (frame._tag === "TurnAccepted") accepted = frame;
        if (frame._tag === "StreamEvent" && frame.durableEvent.event._tag === "TextDelta") {
          partialSequence = frame.durableEvent.sequence;
        }
      }
      if (accepted === undefined || partialSequence < 1) {
        return yield* new LiveSocketFailure({ message: "No partial model output before redeploy" });
      }
      const oldBootId = initial.probe.runtime.bootId;
      yield* closeRuntime(initial.connection);

      const recoveryMarkerValue = `recovery-${crypto.randomUUID()}`;
      const recoveryMarker = `/** Build marker changed only by the credentialed redeploy recovery test. */\nexport const DEPLOYMENT_MARKER = "${recoveryMarkerValue}";\n\n/** Compile-time fault gate used only by the credentialed isolate-loss test deployment. */\nexport const LIVE_CHAOS_ABORT_AFTER_FIRST_DELTA = false;\n`;
      const redeployed = yield* Effect.gen(function* () {
        yield* Effect.promise(() => Bun.write(markerPath, recoveryMarker));
        return yield* deploy(Stack);
      }).pipe(
        Effect.ensuring(
          Effect.promise(() => Bun.write(markerPath, originalMarker)).pipe(Effect.asVoid),
        ),
      );
      expect(redeployed.websiteUrl).toBe(websiteUrl);
      expect(redeployed.backendUrl).toBe(backendUrl);
      yield* waitForDeploymentMarker(backendUrl, recoveryMarkerValue);
      yield* waitForDeploymentMarker(backendUrl, recoveryMarkerValue, sessionId);
      yield* Effect.sleep("20 seconds");

      const resumed = yield* resumeEventually(backendUrl, sessionId, partialSequence);
      expect(resumed.probe.runtime.bootId).not.toBe(oldBootId);
      const result = yield* collectThroughTerminal(resumed.connection, resumed.replayFrames);
      yield* Effect.logInfo("Redeploy terminal evidence", {
        status: result.terminal.status,
        generation: result.terminal.generation,
        attempt: result.terminal.attempt,
        recoveryWork: result.terminal.recoveryWork,
        reason: result.terminal.reason,
      });
      expect(result.terminal.status).toBe("completed");
      expect(result.terminal.generation).toBeGreaterThan(1);
      expect(result.terminal.attempt).toBeGreaterThanOrEqual(1);
      expect(result.terminal.recoveryWork).toBeGreaterThan(0);

      const allSequences = [
        ...streamSequences(observed).filter((sequence) => sequence <= partialSequence),
        ...streamSequences(result.frames),
      ];
      expect(allSequences).toEqual(
        Array.from({ length: result.terminal.sequence + 1 }, (_, index) => index),
      );
      const transcript = yield* client.getSession({ sessionId });
      expect(transcript.messages.filter((message) => message.role === "user").length).toBe(1);
      expect(transcript.messages.filter((message) => message.role === "assistant").length).toBe(1);
      expect((yield* client.getRuntime({ sessionId })).activeOperation).toBeNull();
      yield* closeRuntime(resumed.connection);
    }).pipe(Effect.provide(protocol)),
    { timeout: 900_000 },
  );
}
