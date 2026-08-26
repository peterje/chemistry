# Alchemy starter

GitHub template for a small Alchemy + React demo with the same verification toolchain used in [chemistry](https://github.com/peterje/chemistry):

- Alchemy 2 on Cloudflare Workers (`bun run dev` / `bun run deploy`)
- Bun workspaces, TypeScript 7 strictness, conventional commits
- oxlint **anti-slop** (plus Effect rules), oxfmt, lefthook `bun run check`
- Playwright browser integration on `main`
- React Doctor skill and `bun run doctor`
- GitHub Actions CI/CD and an Alchemy stack that provisions Cloudflare secrets

The app itself is a one-page counter. Use this repo as a GitHub template, then replace the demo UI.

## Commands

```bash
bun install
bun run dev          # http://localhost:1337
bun run check        # format, lint, types, unit tests, build
bun run test:browser # Playwright against alchemy dev
bun run doctor       # React Doctor on the changed scope
bun run deploy       # production Worker
```

Provision GitHub Actions Cloudflare secrets once:

```bash
bun run ci:provision
```

## Layout

```text
apps/website          TanStack Start + React demo
stacks/github.ts      CI credential bootstrap
tools/oxlint          anti-slop plugins
.agents/skills        React Doctor agent skill
.github/workflows     verify → browser → deploy
```
