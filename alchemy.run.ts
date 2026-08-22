import AgentBackend from "@chemistry/backend/agent-backend";
import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";

/** Cloudflare-hosted web application for the agent demo. */
export class Website extends Cloudflare.Website.Vite<Website>()("Website", {
  rootDir: "apps/website",
  memo: {
    include: ["**/*", "../../packages/contracts/src/**", "../../packages/client-runtime/src/**"],
    lockfile: true,
  },
  compatibility: {
    flags: ["nodejs_compat", "enable_request_signal"],
  },
  env: {
    BACKEND: AgentBackend,
  },
}) {}

/** Deployable Alchemy stack for the website and its bound agent backend. */
export default Alchemy.Stack(
  "AlchemyAgent",
  {
    providers: Cloudflare.providers(),
    state: Cloudflare.state(),
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
