import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  BootId,
  ConnectionId,
  OperationId,
  StreamId,
  SubmissionId,
  type BootId as BootIdType,
  type ConnectionId as ConnectionIdType,
  type OperationId as OperationIdType,
  type StreamId as StreamIdType,
  type SubmissionId as SubmissionIdType,
} from "@chemistry/contracts/agent-protocol";

/** Identity factory required by durable runtime orchestration. */
export interface RuntimeIdSourceOperations {
  /** Allocate a client-independent submission id. */
  readonly submission: () => Effect.Effect<SubmissionIdType>;
  /** Allocate one durable operation id. */
  readonly operation: () => Effect.Effect<OperationIdType>;
  /** Allocate one durable stream id. */
  readonly stream: () => Effect.Effect<StreamIdType>;
  /** Allocate one hibernatable connection id. */
  readonly connection: () => Effect.Effect<ConnectionIdType>;
  /** Allocate one Durable Object boot id. */
  readonly boot: () => Effect.Effect<BootIdType>;
  /** Allocate one recovery incident id. */
  readonly incident: () => Effect.Effect<string>;
}

/** Effect service for durable runtime identities. */
export class RuntimeIdSource extends Context.Service<RuntimeIdSource, RuntimeIdSourceOperations>()(
  "@chemistry/RuntimeIdSource",
) {}

const uuid = () => crypto.randomUUID();

/** Cryptographically random runtime identity source used by Cloudflare workers. */
export const RuntimeIdSourceLive = Layer.succeed(
  RuntimeIdSource,
  RuntimeIdSource.of({
    submission: Effect.fn("RuntimeIdSource.submission")(() =>
      Effect.sync(() => SubmissionId.make(`submission-${uuid()}`)),
    ),
    operation: Effect.fn("RuntimeIdSource.operation")(() =>
      Effect.sync(() => OperationId.make(`operation-${uuid()}`)),
    ),
    stream: Effect.fn("RuntimeIdSource.stream")(() =>
      Effect.sync(() => StreamId.make(`stream-${uuid()}`)),
    ),
    connection: Effect.fn("RuntimeIdSource.connection")(() =>
      Effect.sync(() => ConnectionId.make(`connection-${uuid()}`)),
    ),
    boot: Effect.fn("RuntimeIdSource.boot")(() => Effect.sync(() => BootId.make(`boot-${uuid()}`))),
    incident: Effect.fn("RuntimeIdSource.incident")(() => Effect.sync(() => `incident-${uuid()}`)),
  }),
);
