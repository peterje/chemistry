import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { SessionId } from "../shared/agent-protocol.ts";
import type { RuntimeState } from "./runtime-state.ts";

/** Runtime persistence failed or a stored runtime record could not be decoded. */
export class RuntimePersistenceError extends Schema.TaggedError<RuntimePersistenceError>()(
  "RuntimePersistenceError",
  {
    operation: Schema.String,
    message: Schema.String,
  },
) {}

/** A requested runtime transition is illegal for the durable state. */
export class RuntimeTransitionError extends Schema.TaggedError<RuntimeTransitionError>()(
  "RuntimeTransitionError",
  {
    operation: Schema.String,
    message: Schema.String,
  },
) {}

/** A bounded runtime queue, stream, or retained-record limit was reached. */
export class RuntimeCapacityError extends Schema.TaggedError<RuntimeCapacityError>()(
  "RuntimeCapacityError",
  {
    resource: Schema.String,
    limit: Schema.Number,
    message: Schema.String,
  },
) {}

/** A replay cursor points beyond the latest durable stream sequence. */
export class RuntimeCursorError extends Schema.TaggedError<RuntimeCursorError>()(
  "RuntimeCursorError",
  {
    streamId: Schema.String,
    requestedSequence: Schema.Number,
    latestSequence: Schema.Number,
    message: Schema.String,
  },
) {}

/** A stale execution owner attempted a generation-fenced mutation. */
export class RuntimeFenceError extends Schema.TaggedError<RuntimeFenceError>()(
  "RuntimeFenceError",
  {
    operationId: Schema.String,
    expectedGeneration: Schema.Number,
    receivedGeneration: Schema.Number,
    message: Schema.String,
  },
) {}

/** New durable state and application result produced by one atomic mutation. */
export interface RuntimeMutation<A> {
  /** Complete replacement runtime state. */
  readonly state: RuntimeState;
  /** Value returned to the application caller after commit. */
  readonly value: A;
}

/** Persistence capability required by the durable execution application module. */
export interface RuntimeStoreOperations {
  /** Load or initialize a versioned runtime record. */
  readonly load: (
    sessionId: SessionId,
    initial: RuntimeState,
  ) => Effect.Effect<RuntimeState, RuntimePersistenceError>;
  /** Apply and commit one state transition atomically. */
  readonly transact: <A, E>(
    operation: string,
    sessionId: SessionId,
    initial: RuntimeState,
    mutation: (state: RuntimeState) => Effect.Effect<RuntimeMutation<A>, E>,
  ) => Effect.Effect<A, E | RuntimePersistenceError>;
}

/** Effect service for versioned durable runtime persistence. */
export class RuntimeStore extends Context.Service<RuntimeStore, RuntimeStoreOperations>()(
  "@chemistry/RuntimeStore",
) {}
