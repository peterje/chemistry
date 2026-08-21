import { createRouter } from "@tanstack/react-router";
import { routeTree } from "./routeTree.ts";

/** Create the TanStack router used by the browser and server renderers. */
export function getRouter() {
  return createRouter({
    routeTree,
    scrollRestoration: true,
  });
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof getRouter>;
  }
}
