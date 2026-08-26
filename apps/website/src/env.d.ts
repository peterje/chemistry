export {};

declare module "cloudflare:workers" {
  namespace Cloudflare {
    /** Website Worker bindings available only at the server adapter boundary. */
    interface Env {}
  }
}
