import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as LanguageModel from "effect/unstable/ai/LanguageModel";
import type * as Response from "effect/unstable/ai/Response";
import {
  AgentCompactionError,
  AgentInferenceError,
  AgentStreamEvent,
  Compaction,
  CompactionResult,
  ToolLoopLimitExceeded,
  TranscriptMessage,
  type AgentContext,
  type AgentRpcError,
  type MessageId,
  type SessionId,
} from "@chemistry/contracts/agent-protocol";
import { AgentService, defaultAgentConfiguration } from "./agent-service.ts";
import { AgentToolkit, AgentToolkitLive } from "./agent-toolkit.ts";
import { buildCompactionPlan } from "./chat-compaction.ts";
import {
  agentEventFromResponsePart,
  assembleContinuationPrompt,
  assembleModelPrompt,
  compactionStats,
  estimateModelTokens,
  createInitialSession,
  snapshot,
  textMessage,
  transcriptSegmentsFromResponse,
} from "./conversation.ts";
import { MessageIdSource } from "./message-id-source.ts";
import { SessionStore, type StoredSession } from "./session-store.ts";
import type { PersistPartialInput, TurnExecutionInput } from "./turn-executor.ts";

const requiredFactTool = { tool: "lookup_project_fact" } as const;

const inferenceError = (operation: string, cause: unknown) =>
  new AgentInferenceError({
    operation,
    message: cause instanceof Error ? cause.message : String(cause),
  });

/** Live application Layer for durable messaging and context operations. */
export const AgentServiceLive = Layer.effect(
  AgentService,
  Effect.gen(function* () {
    const store = yield* SessionStore;
    const ids = yield* MessageIdSource;
    const model = yield* LanguageModel.LanguageModel;
    const semaphore = yield* Semaphore.make(1);

    const load = Effect.fn("AgentService.load")(function* (sessionId: SessionId) {
      return yield* store.getOrCreate(sessionId, createInitialSession(sessionId));
    });

    const withLock = <A, E>(effect: Effect.Effect<A, E>) => semaphore.withPermits(1)(effect);

    const getSession = Effect.fn("AgentService.getSession")(function* (sessionId: SessionId) {
      return snapshot(yield* load(sessionId));
    }, withLock);

    const updateContext = Effect.fn("AgentService.updateContext")(function* (
      sessionId: SessionId,
      context: AgentContext,
    ) {
      const current = yield* load(sessionId);
      const updated: StoredSession = { ...current, context };
      yield* store.save(updated);
      return snapshot(updated);
    }, withLock);

    const compact = Effect.fn("AgentService.compact")(function* (
      current: StoredSession,
      trigger: "manual" | "threshold",
    ) {
      const before = compactionStats(current);
      if (
        trigger === "threshold" &&
        estimateModelTokens(current) <= defaultAgentConfiguration.compactionThresholdTokens
      ) {
        return {
          session: current,
          result: CompactionResult.make({
            compacted: false,
            reason: "below-threshold",
            stats: before,
          }),
        };
      }

      const plan = buildCompactionPlan(current, defaultAgentConfiguration.retainRecentMessages);
      if (plan === undefined) {
        return {
          session: current,
          result: CompactionResult.make({
            compacted: false,
            reason: "insufficient-history",
            stats: before,
          }),
        };
      }

      const response = yield* model.generateText({ prompt: plan.prompt, toolChoice: "none" }).pipe(
        Effect.mapError(
          (cause) =>
            new AgentCompactionError({
              message:
                cause instanceof Error
                  ? cause.message
                  : `Compaction inference failed: ${String(cause)}`,
            }),
        ),
      );
      const summary = response.text.trim();
      if (summary.length === 0) {
        return yield* new AgentCompactionError({
          message: "Compaction model returned an empty summary",
        });
      }

      const compactionId = yield* ids.next();
      const createdAt = yield* Clock.currentTimeMillis;
      const overlay = Compaction.make({
        id: compactionId,
        fromMessageId: plan.fromMessage.id,
        toMessageId: plan.toMessage.id,
        summary,
        sourceMessageCount: plan.sourceMessageCount,
        createdAt,
      });
      const updated: StoredSession = {
        ...current,
        compactions: [...current.compactions, overlay],
      };
      yield* store.save(updated);
      return {
        session: updated,
        result: CompactionResult.make({
          compacted: true,
          reason: trigger,
          stats: compactionStats(updated),
        }),
      };
    });

    const compactSession = Effect.fn("AgentService.compactSession")(function* (
      sessionId: SessionId,
    ) {
      return (yield* compact(yield* load(sessionId), "manual")).result;
    }, withLock);

    const persistResponse = Effect.fn("AgentService.persistResponse")(function* (
      active: StoredSession,
      parts: ReadonlyArray<Response.AnyPart>,
      preferredAssistantId?: MessageId,
    ) {
      const segments = yield* transcriptSegmentsFromResponse(parts);
      let messages: ReadonlyArray<TranscriptMessage> = active.messages;
      let assistant: TranscriptMessage | undefined;
      let availableAssistantId = preferredAssistantId;

      for (const segment of segments) {
        const createdAt = yield* Clock.currentTimeMillis;
        if (segment.role === "assistant" && availableAssistantId !== undefined) {
          const existing = messages.find((message) => message.id === availableAssistantId);
          const message = TranscriptMessage.make({
            id: availableAssistantId,
            role: "assistant",
            parts: existing === undefined ? segment.parts : [...existing.parts, ...segment.parts],
            createdAt: existing?.createdAt ?? createdAt,
          });
          messages =
            existing === undefined
              ? [...messages, message]
              : messages.map((candidate) =>
                  candidate.id === availableAssistantId ? message : candidate,
                );
          assistant = message;
          availableAssistantId = undefined;
          continue;
        }
        const id = yield* ids.next();
        const message = TranscriptMessage.make({
          id,
          role: segment.role,
          parts: segment.parts,
          createdAt,
        });
        messages = [...messages, message];
        if (message.role === "assistant") assistant = message;
      }

      const updated: StoredSession = { ...active, messages };
      yield* store.save(updated);
      return { session: updated, assistant };
    });

    const liveEventsFromParts = Effect.fn("AgentService.liveEventsFromParts")(function* (
      parts: ReadonlyArray<Response.AnyPart>,
    ) {
      const events = yield* Effect.forEach(parts, agentEventFromResponsePart);
      const present: Array<AgentStreamEvent> = [];
      for (const event of events) {
        if (event !== undefined) present.push(event);
      }
      return present;
    });

    function continueAfterResponse(
      active: StoredSession,
      originalPrompt: string,
      step: number,
      previousAssistant: TranscriptMessage | undefined,
      parts: ReadonlyArray<Response.AnyPart>,
      preferredAssistantId: MessageId | undefined,
    ): Effect.Effect<Stream.Stream<AgentStreamEvent, AgentRpcError>, AgentRpcError> {
      return Effect.gen(function* () {
        const persisted = yield* persistResponse(active, parts, preferredAssistantId);
        const assistant = persisted.assistant ?? previousAssistant;
        const calledTool = parts.some((part) => part.type === "tool-call");

        if (calledTool) {
          if (step >= defaultAgentConfiguration.maxToolSteps) {
            return Stream.fail(
              new ToolLoopLimitExceeded({
                maxSteps: defaultAgentConfiguration.maxToolSteps,
                message: `Tool loop exceeded ${defaultAgentConfiguration.maxToolSteps} steps`,
              }),
            );
          }
          return streamRound(
            persisted.session,
            originalPrompt,
            step + 1,
            assistant,
            undefined,
            "fresh",
          );
        }

        if (assistant !== undefined) {
          return Stream.make(
            AgentStreamEvent.cases.TurnCompleted.make({
              assistantMessage: assistant,
              stats: compactionStats(persisted.session),
            }),
          );
        }

        const id = preferredAssistantId ?? (yield* ids.next());
        const createdAt = yield* Clock.currentTimeMillis;
        const emptyAssistant = textMessage(id, "assistant", "", createdAt);
        const completed: StoredSession = {
          ...persisted.session,
          messages: [...persisted.session.messages, emptyAssistant],
        };
        yield* store.save(completed);
        return Stream.make(
          AgentStreamEvent.cases.TurnCompleted.make({
            assistantMessage: emptyAssistant,
            stats: compactionStats(completed),
          }),
        );
      });
    }

    function streamRound(
      active: StoredSession,
      originalPrompt: string,
      step: number,
      previousAssistant: TranscriptMessage | undefined,
      preferredAssistantId: MessageId | undefined,
      mode: "fresh" | "continue",
    ): Stream.Stream<AgentStreamEvent, AgentRpcError> {
      const prompt =
        mode === "continue" ? assembleContinuationPrompt(active) : assembleModelPrompt(active);
      const requiresFactTool =
        mode === "fresh" && step === 1 && originalPrompt.includes("lookup_project_fact");
      if (requiresFactTool) {
        return Stream.unwrap(
          Effect.gen(function* () {
            const response = yield* model
              .generateText({
                prompt,
                toolkit: AgentToolkit,
                toolChoice: requiredFactTool,
              })
              .pipe(
                Effect.provide(AgentToolkitLive),
                Effect.mapError((cause) => inferenceError("generate-tool-call", cause)),
              );
            const events = yield* liveEventsFromParts(response.content);
            const continuation = yield* continueAfterResponse(
              active,
              originalPrompt,
              step,
              previousAssistant,
              response.content,
              preferredAssistantId,
            );
            return Stream.concat(Stream.fromIterable(events), continuation);
          }),
        );
      }

      const hasResponseContent = (parts: ReadonlyArray<Response.AnyPart>): boolean =>
        parts.some((part) => {
          switch (part.type) {
            case "text":
              return part.text.length > 0;
            case "text-delta":
              return part.delta.length > 0;
            case "tool-call":
            case "tool-result":
              return true;
            default:
              return false;
          }
        });

      const streamResponse = (
        includeToolkit: boolean,
      ): Stream.Stream<AgentStreamEvent, AgentRpcError> =>
        Stream.unwrap(
          Effect.gen(function* () {
            const captured = yield* Ref.make<ReadonlyArray<Response.AnyPart>>([]);
            const modelStream = includeToolkit
              ? model
                  .streamText({
                    prompt,
                    toolkit: AgentToolkit,
                    toolChoice: originalPrompt.includes("lookup_project_fact") ? "none" : "auto",
                  })
                  .pipe(Stream.provide(AgentToolkitLive))
              : model.streamText({ prompt, toolChoice: "none" });
            const responseStream = modelStream.pipe(
              Stream.mapError((cause) => inferenceError("stream-text", cause)),
              Stream.tap((part) => Ref.update(captured, (parts) => [...parts, part])),
              Stream.mapEffect(agentEventFromResponsePart),
              Stream.filter((event): event is AgentStreamEvent => event !== undefined),
            );
            const continuation = Stream.unwrap(
              Ref.get(captured).pipe(
                Effect.flatMap((parts) =>
                  !hasResponseContent(parts)
                    ? Effect.fail(
                        new AgentInferenceError({
                          operation: "empty-model-stream",
                          message: "Model stream completed without text or tool output",
                        }),
                      )
                    : continueAfterResponse(
                        active,
                        originalPrompt,
                        step,
                        previousAssistant,
                        parts,
                        preferredAssistantId,
                      ),
                ),
              ),
            );
            return Stream.concat(responseStream, continuation);
          }),
        );

      return streamResponse(mode === "fresh");
    }

    const prepareLegacyTurn = Effect.fn("AgentService.prepareLegacyTurn")(function* (
      sessionId: SessionId,
      prompt: string,
    ) {
      const loaded = yield* load(sessionId);
      const automaticCompaction = yield* compact(loaded, "threshold");
      const userId = yield* ids.next();
      const userTime = yield* Clock.currentTimeMillis;
      const userMessage = textMessage(userId, "user", prompt, userTime);
      const withUser: StoredSession = {
        ...automaticCompaction.session,
        messages: [...automaticCompaction.session.messages, userMessage],
      };
      yield* store.save(withUser);
      const prefix = [
        ...(automaticCompaction.result.compacted
          ? [
              AgentStreamEvent.cases.CompactionCompleted.make({
                result: automaticCompaction.result,
              }),
            ]
          : []),
        AgentStreamEvent.cases.TurnStarted.make({ userMessage }),
      ];
      return Stream.concat(
        Stream.fromIterable(prefix),
        streamRound(withUser, prompt, 1, undefined, undefined, "fresh"),
      );
    });

    const prepareRuntimeTurn = Effect.fn("AgentService.prepareRuntimeTurn")(function* (
      input: TurnExecutionInput,
    ) {
      const loaded = yield* load(input.sessionId);
      if (input.mode === "continue") {
        const continuedSession: StoredSession = {
          ...loaded,
          context: input.requestSnapshot.context,
        };
        const previousAssistant = continuedSession.messages.find(
          (message) => message.id === input.assistantMessageId,
        );
        return streamRound(
          continuedSession,
          input.prompt,
          1,
          previousAssistant,
          input.assistantMessageId,
          "continue",
        );
      }

      const historyIds = new Set(input.requestSnapshot.historyMessageIds);
      const compactionIds = new Set(input.requestSnapshot.compactionIds);
      const capturedSession: StoredSession = {
        ...loaded,
        context: input.requestSnapshot.context,
        messages: loaded.messages.filter((message) => historyIds.has(message.id)),
        compactions: loaded.compactions.filter((compaction) => compactionIds.has(compaction.id)),
      };
      const existingUser = capturedSession.messages.find(
        (message) => message.id === input.userMessageId,
      );
      const automaticCompaction =
        existingUser === undefined
          ? yield* compact(capturedSession, "threshold")
          : {
              session: capturedSession,
              result: CompactionResult.make({
                compacted: false,
                reason: "below-threshold",
                stats: compactionStats(capturedSession),
              }),
            };
      const userTime = yield* Clock.currentTimeMillis;
      const userMessage =
        existingUser ?? textMessage(input.userMessageId, "user", input.prompt, userTime);
      const withUser: StoredSession =
        existingUser === undefined
          ? {
              ...automaticCompaction.session,
              messages: [...automaticCompaction.session.messages, userMessage],
            }
          : automaticCompaction.session;
      if (existingUser === undefined) yield* store.save(withUser);
      const prefix = [
        ...(automaticCompaction.result.compacted
          ? [
              AgentStreamEvent.cases.CompactionCompleted.make({
                result: automaticCompaction.result,
              }),
            ]
          : []),
        AgentStreamEvent.cases.TurnStarted.make({ userMessage }),
      ];
      return Stream.concat(
        Stream.fromIterable(prefix),
        streamRound(withUser, input.prompt, 1, undefined, input.assistantMessageId, "fresh"),
      );
    });

    const sendMessage = (
      sessionId: SessionId,
      prompt: string,
    ): Stream.Stream<AgentStreamEvent, AgentRpcError> =>
      Stream.scoped(
        Stream.unwrap(
          Effect.acquireRelease(semaphore.take(1), () => semaphore.release(1)).pipe(
            Effect.flatMap(() => prepareLegacyTurn(sessionId, prompt)),
          ),
        ),
      );

    const runTurn = (input: TurnExecutionInput): Stream.Stream<AgentStreamEvent, AgentRpcError> =>
      Stream.scoped(
        Stream.unwrap(
          Effect.acquireRelease(semaphore.take(1), () => semaphore.release(1)).pipe(
            Effect.flatMap(() => prepareRuntimeTurn(input)),
          ),
        ),
      );

    const persistPartial = Effect.fn("AgentService.persistPartial")(function* (
      input: PersistPartialInput,
    ) {
      const current = yield* load(input.sessionId);
      const existing = current.messages.find((message) => message.id === input.assistantMessageId);
      const createdAt = existing?.createdAt ?? (yield* Clock.currentTimeMillis);
      const partial = textMessage(input.assistantMessageId, "assistant", input.text, createdAt);
      const updated: StoredSession = {
        ...current,
        messages:
          existing === undefined
            ? [...current.messages, partial]
            : current.messages.map((message) =>
                message.id === input.assistantMessageId ? partial : message,
              ),
      };
      yield* store.save(updated);
    }, withLock);

    const hasAssistantMessage = Effect.fn("AgentService.hasAssistantMessage")(function* (
      sessionId: SessionId,
      assistantMessageId: MessageId,
    ) {
      return (yield* load(sessionId)).messages.some(
        (message) => message.id === assistantMessageId && message.role === "assistant",
      );
    }, withLock);

    return AgentService.of({
      getSession,
      updateContext,
      sendMessage,
      runTurn,
      persistPartial,
      hasAssistantMessage,
      compactSession,
    });
  }),
);
