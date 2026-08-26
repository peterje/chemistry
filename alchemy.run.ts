import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";

/** Cloudflare-hosted Vite website for the starter demo. */
export class Website extends Cloudflare.Website.Vite<Website>()("Website", {
  rootDir: "apps/website",
  memo: {
    include: ["**/*"],
    lockfile: true,
  },
  compatibility: {
    flags: ["nodejs_compat"],
  },
}) {}

/** Deployable Alchemy stack for the demo website. */
export default Alchemy.Stack(
  "Starter",
  {
    providers: Cloudflare.providers(),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const website = yield* Website;
    return {
      websiteUrl: website.url.as<string>(),
    };
  }),
);
