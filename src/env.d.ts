import type { WebsiteEnv } from "../alchemy.run.ts";

declare module "cloudflare:workers" {
  namespace Cloudflare {
    interface Env extends WebsiteEnv {}
  }
}
