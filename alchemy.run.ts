import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import AgentBackend from "./src/server/agent-backend.ts";

/** Cloudflare-hosted web application for the agent demo. */
export class Website extends Cloudflare.Website.Vite<Website>()("Website", {
  compatibility: {
    flags: ["nodejs_compat", "enable_request_signal"],
  },
  env: {
    BACKEND: AgentBackend,
  },
}) {}

/** Runtime bindings exposed to the TanStack Start website Worker. */
export type WebsiteEnv = Cloudflare.InferEnv<typeof Website>;

/** Deployable Alchemy stack for the website and its bound agent backend. */
export default Alchemy.Stack(
  "AlchemyAgent",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const backend = yield* AgentBackend;
    const website = yield* Website;
    return {
      backendUrl: backend.url.as<string>(),
      websiteUrl: website.url.as<string>(),
    };
  }),
);
