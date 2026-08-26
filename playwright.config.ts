import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./apps/website/test/browser",
  testMatch: "**/*.pw.ts",
  fullyParallel: false,
  workers: 1,
  timeout: 60_000,
  expect: { timeout: 10_000 },
  use: {
    baseURL: "http://localhost:1337",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "bun run dev",
    url: "http://localhost:1337",
    reuseExistingServer: true,
    timeout: 120_000,
  },
});
