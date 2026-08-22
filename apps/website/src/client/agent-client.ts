import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as AtomRpc from "effect/unstable/reactivity/AtomRpc";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import { AgentRpcs, type SessionId } from "@chemistry/contracts/agent-protocol";

/** Browser transport for the same-origin Effect RPC endpoint. */
const agentRpcProtocol = RpcClient.layerProtocolHttp({
  url: "/rpc",
}).pipe(Layer.provide(FetchHttpClient.layer), Layer.provide(RpcSerialization.layerNdjson));

/** Reactive browser client generated from the shared agent RPC group. */
export class AgentClient extends AtomRpc.Service<AgentClient>()("AgentClient", {
  group: AgentRpcs,
  protocol: agentRpcProtocol,
}) {}

/** Reactive query for the durable conversation navigation catalog. */
export const chatListAtom = AgentClient.query(
  "listChats",
  {},
  {
    reactivityKeys: ["chats"],
  },
);

/** Create a durable conversation before navigating to its canonical route. */
export const createDurableChat = Effect.fn("AgentClient.createDurableChat")(function* (
  sessionId: SessionId,
) {
  return yield* RpcClient.make(AgentRpcs).pipe(
    Effect.flatMap((client) => client.createChat({ sessionId })),
    Effect.provide(agentRpcProtocol),
    Effect.scoped,
  );
});

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
