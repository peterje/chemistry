import { createFileRoute } from "@tanstack/react-router";
import { env } from "cloudflare:workers";

/** Same-origin framework boundary forwarding typed Effect RPC frames. */
export const Route = createFileRoute("/rpc")({
  server: {
    handlers: {
      ANY: ({ request }) => env.BACKEND.fetch(request),
    },
  },
});
