import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import { defineConfig } from "vite";

/** Vite and TanStack Start build configuration for the Cloudflare website. */
export default defineConfig({
  plugins: [
    tanstackStart({
      router: {
        generatedRouteTree: "./.tanstack/routeTree.gen.ts",
      },
    }),
    viteReact(),
  ],
  build: {
    rollupOptions: {
      external: ["cloudflare:workers"],
    },
  },
});
