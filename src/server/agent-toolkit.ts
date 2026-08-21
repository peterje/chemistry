import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

/** Deterministic fact lookup available to the demonstration agent. */
export const LookupProjectFact = Tool.make("lookup_project_fact", {
  description:
    "Look up an authoritative fact about this demonstration's runtime, persistence, or protocol.",
  parameters: Schema.Struct({
    topic: Schema.Literals(["runtime", "persistence", "protocol"]),
  }),
  success: Schema.Struct({
    topic: Schema.String,
    fact: Schema.String,
  }),
});

/** Toolkit exposed to native Workers AI during agent turns. */
export const AgentToolkit = Toolkit.make(LookupProjectFact);

const facts = {
  runtime: "The agent runs on Cloudflare Workers and a per-session Durable Object.",
  persistence:
    "Raw transcript, context, and non-destructive compaction overlays live in Durable Object storage.",
  protocol: "The browser and server share one Effect RpcGroup and communicate with NDJSON framing.",
} satisfies Record<"runtime" | "persistence" | "protocol", string>;

/** Live deterministic handlers for the demonstration toolkit. */
export const AgentToolkitLive = AgentToolkit.toLayer({
  lookup_project_fact: ({ topic }) =>
    Effect.succeed({
      topic,
      fact: facts[topic],
    }),
});
