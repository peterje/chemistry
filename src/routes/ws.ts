import { createFileRoute } from "@tanstack/react-router";
import { env } from "cloudflare:workers";

/** Same-origin WebSocket boundary forwarding upgrades to the private agent backend. */
export const Route = createFileRoute("/ws")({
  server: {
    handlers: {
      GET: ({ request }) => env.BACKEND.fetch(request),
    },
  },
});
