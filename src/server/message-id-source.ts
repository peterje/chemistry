import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as IdGenerator from "effect/unstable/ai/IdGenerator";
import { MessageId } from "../shared/agent-protocol.ts";

/** Identifier capability used by transcript and compaction creation. */
export interface MessageIdSourceOperations {
  /** Generate one valid message identifier. */
  readonly next: () => Effect.Effect<MessageId>;
}

/** Injectable transcript identifier source. */
export class MessageIdSource extends Context.Service<MessageIdSource, MessageIdSourceOperations>()(
  "@alchemy-agent/MessageIdSource",
) {}

/** Live identifier source backed by Effect AI's default ID generator. */
export const MessageIdSourceLive = Layer.succeed(
  MessageIdSource,
  MessageIdSource.of({
    next: Effect.fn("MessageIdSource.next")(function* () {
      const id = yield* IdGenerator.defaultIdGenerator.generateId();
      return MessageId.make(id);
    }),
  }),
);
