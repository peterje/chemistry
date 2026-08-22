# AGENTS.md

## Cursor Cloud specific instructions

This is a Bun workspace (`chemistry`) — an Effect 4 / Alchemy 2 / Cloudflare agent runtime.
Standard scripts live in `package.json`; setup/run/verify details are in `README.md`.
The update script already runs `bun install --frozen-lockfile` on boot, so dependencies
are ready when a new agent starts.

### Toolchain / PATH gotcha (important)

- The package manager is **Bun 1.3.14** (pinned by CI). It is installed at `$HOME/.bun/bin`.
- `oxlint` (used by `bun run lint` and `bun run check`) loads a **TypeScript** config
  (`oxlint.config.ts`) and therefore requires **Node.js >= 22.18.0**. The raw daemon shell's
  default `node` on `PATH` (`/exec-daemon/node`) is **v22.14.0** and makes lint fail with
  `TypeScript config files require Node.js ^20.19.0 || >=22.18.0`.
- `nvm` provides **v22.22.2** as the default and satisfies this. A login shell (`bash -l`)
  resolves the correct node automatically. In a plain non-login command shell, prepend the
  nvm bin first:
  `export PATH="$HOME/.nvm/versions/node/v22.22.2/bin:$HOME/.bun/bin:$PATH"`.
  Verify with `node --version` → should be `v22.22.2`, not `v22.14.0`.

### Credential-free checks (work without any secrets)

- `bun run lint`, `bun run typecheck`, `bun test` (75 deterministic tests; 1 live E2E is
  skipped unless `RUN_LIVE_E2E=1`), `bun run format:check`, and `bun run build` all pass
  offline. `bun run check` runs the whole chain.

### Cloudflare authentication for app / browser / live tests

- Local development uses an Alchemy profile. Run `bun alchemy login --configure`, choose
  **Cloudflare OAuth**, and complete the browser sign-in. Alchemy stores the credentials in
  `~/.alchemy/profiles.json`; inspect the active profile with secrets redacted via
  `bun alchemy profile show`. This is the preferred local setup.
- `CLOUDFLARE_API_TOKEN` and a valid 32-hex `CLOUDFLARE_ACCOUNT_ID` are primarily for CI
  and other non-interactive environments. Set `CI=1` when using environment credentials so
  Alchemy does not try to prompt.
- The app uses a remote Cloudflare state store (`Cloudflare.state()`) and native Workers AI
  (`@cf/zai-org/glm-5.2`), so either an authenticated local profile or environment
  credentials must grant access to the required account services.
- Local dev pins the Website to `http://localhost:1337` and `AgentBackend` to
  `http://localhost:1338`. Opening `/` durably creates a chat and redirects to
  `/chat/:chatId`; sending a message triggers Workers AI inference (needs the account).
- `bun run test:browser` (Playwright) starts `bun run dev` as its webServer, so it needs the
  same Cloudflare credentials. The Chromium browser is already installed via
  `bunx playwright install chromium`.
- `bun run test:e2e` (`RUN_LIVE_E2E=1`) deploys with Alchemy and also needs the credentials.
