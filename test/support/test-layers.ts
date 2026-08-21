import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as LanguageModel from "effect/unstable/ai/LanguageModel";
import type * as Response from "effect/unstable/ai/Response";
import { MessageId } from "../../src/shared/agent-protocol.ts";
import { MessageIdSource } from "../../src/server/message-id-source.ts";
import { SessionStore, type StoredSession } from "../../src/server/session-store.ts";

/** Captured provider calls exposed to focused agent tests. */
export interface TestLanguageModelObservations {
  /** Return every normalized provider request observed by the fake model. */
  readonly requests: () => Effect.Effect<ReadonlyArray<LanguageModel.ProviderOptions>>;
}

/** Test-only control surface paired with the fake LanguageModel service. */
export class TestLanguageModel extends Context.Service<
  TestLanguageModel,
  TestLanguageModelObservations
>()("@alchemy-agent/TestLanguageModel") {}

/** Build an in-memory SessionStore Layer with production-compatible semantics. */
export const InMemorySessionStore = Layer.effect(
  SessionStore,
  Effect.gen(function* () {
    const sessions = yield* Ref.make<ReadonlyMap<string, StoredSession>>(new Map());
    return SessionStore.of({
      getOrCreate: (sessionId, initial) =>
        Ref.modify(sessions, (state) => {
          const existing = state.get(sessionId);
          if (existing !== undefined) return [existing, state];
          const next = new Map(state);
          next.set(sessionId, initial);
          return [initial, next];
        }),
      save: (session) =>
        Ref.update(sessions, (state) => {
          const next = new Map(state);
          next.set(session.sessionId, session);
          return next;
        }),
    });
  }),
);

/** Deterministic message IDs for behavior-focused tests. */
export const DeterministicMessageIds = Layer.effect(
  MessageIdSource,
  Effect.gen(function* () {
    const nextId = yield* Ref.make(0);
    return MessageIdSource.of({
      next: () =>
        Ref.updateAndGet(nextId, (value) => value + 1).pipe(
          Effect.map((value) => MessageId.make(`message-${value}`)),
        ),
    });
  }),
);

type TestModelPart =
  | Extract<Response.PartEncoded, { readonly type: "text" }>
  | Extract<Response.PartEncoded, { readonly type: "tool-call" }>;

/** Provider-hook response function used by the fake LanguageModel Layer. */
export type TestModelResponder = (
  request: LanguageModel.ProviderOptions,
  requestIndex: number,
) => ReadonlyArray<TestModelPart>;

const streamingParts = (
  parts: ReadonlyArray<TestModelPart>,
  requestIndex: number,
): ReadonlyArray<Response.StreamPartEncoded> => {
  const output: Array<Response.StreamPartEncoded> = [];
  for (const part of parts) {
    if (part.type === "text") {
      const id = `text-${requestIndex}`;
      const midpoint = Math.max(1, Math.ceil(part.text.length / 2));
      const deltas = [part.text.slice(0, midpoint), part.text.slice(midpoint)];
      output.push({ type: "text-start", id });
      for (const delta of deltas) {
        if (delta.length > 0) output.push({ type: "text-delta", id, delta });
      }
      output.push({ type: "text-end", id });
    } else {
      output.push(part);
    }
  }
  return output;
};

/** Build a fake LanguageModel plus an observation service over the same Ref. */
export const makeTestLanguageModel = (
  respond: TestModelResponder,
): Layer.Layer<LanguageModel.LanguageModel | TestLanguageModel> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const requests = yield* Ref.make<ReadonlyArray<LanguageModel.ProviderOptions>>([]);
      const model = yield* LanguageModel.make({
        generateText: (request) =>
          Ref.modify(requests, (observed) => {
            const response = [...respond(request, observed.length)];
            return [response, [...observed, request]];
          }),
        streamText: (request) =>
          Stream.unwrap(
            Ref.modify(requests, (observed) => {
              const response = streamingParts(respond(request, observed.length), observed.length);
              return [Stream.fromIterable(response), [...observed, request]];
            }),
          ),
      });
      const testModel = TestLanguageModel.of({
        requests: () => Ref.get(requests),
      });
      return Context.empty().pipe(
        Context.add(LanguageModel.LanguageModel, model),
        Context.add(TestLanguageModel, testModel),
      );
    }),
  );
