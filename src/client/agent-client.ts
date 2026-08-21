import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as AtomRpc from "effect/unstable/reactivity/AtomRpc";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import { AgentRpcs, type SessionId } from "../shared/agent-protocol.ts";

/** Browser transport for the same-origin Effect RPC endpoint. */
const agentRpcProtocol = RpcClient.layerProtocolHttp({
  url: "/rpc",
}).pipe(Layer.provide(FetchHttpClient.layer), Layer.provide(RpcSerialization.layerNdjson));

/** Reactive browser client generated from the shared agent RPC group. */
export class AgentClient extends AtomRpc.Service<AgentClient>()("AgentClient", {
  group: AgentRpcs,
  protocol: agentRpcProtocol,
}) {}

/** Build a reactive session query atom for the selected durable session. */
export const sessionSnapshotAtom = (sessionId: SessionId) =>
  AgentClient.query(
    "getSession",
    { sessionId },
    {
      reactivityKeys: [`session:${sessionId}`],
    },
  );

/** Mutation atom for replacing durable per-session context. */
export const updateContextAtom = AgentClient.mutation("updateContext");

/** Mutation atom for manually compacting eligible session history. */
export const compactSessionAtom = AgentClient.mutation("compactSession");

const acquireAgentStreamClient = RpcClient.make(AgentRpcs);

type AgentStreamClientOperations = Effect.Success<typeof acquireAgentStreamClient>;

/** Scoped non-reactive RPC client for streamed message turns. */
export class AgentStreamClient extends Context.Service<
  AgentStreamClient,
  AgentStreamClientOperations
>()("AgentStreamClient") {}

/** Browser-backed Layer for streamed message turns. */
export const AgentStreamClientLive = Layer.effect(AgentStreamClient, acquireAgentStreamClient).pipe(
  Layer.provide(agentRpcProtocol),
);
