import { expect, test } from "bun:test";

const policies = [
  {
    directory: "packages/contracts",
    packageName: "@chemistry/contracts",
    allowed: new Set<string>(),
  },
  {
    directory: "packages/chat-catalog",
    packageName: "@chemistry/chat-catalog",
    allowed: new Set(["@chemistry/contracts"]),
  },
  {
    directory: "packages/agent-runtime",
    packageName: "@chemistry/agent-runtime",
    allowed: new Set(["@chemistry/contracts"]),
  },
  {
    directory: "packages/client-runtime",
    packageName: "@chemistry/client-runtime",
    allowed: new Set(["@chemistry/contracts"]),
  },
  {
    directory: "apps/backend",
    packageName: "@chemistry/backend",
    allowed: new Set([
      "@chemistry/agent-runtime",
      "@chemistry/chat-catalog",
      "@chemistry/contracts",
    ]),
  },
  {
    directory: "apps/website",
    packageName: "@chemistry/website",
    allowed: new Set(["@chemistry/client-runtime", "@chemistry/contracts"]),
  },
] as const;

const sourceImportPattern = /(?:from\s+|import\s*\()(["'])([^"']+)\1/g;

const workspacePackageName = (specifier: string): string | undefined => {
  if (!specifier.startsWith("@chemistry/")) return undefined;
  const [scope, name] = specifier.split("/");
  return scope === undefined || name === undefined ? undefined : `${scope}/${name}`;
};

test("workspace dependency direction is acyclic and source imports use package interfaces", async () => {
  for (const policy of policies) {
    const manifest = await Bun.file(`${policy.directory}/package.json`).text();
    expect(manifest).toContain(`"name": "${policy.packageName}"`);

    const glob = new Bun.Glob("**/*.{ts,tsx}");
    for await (const relativePath of glob.scan(`${policy.directory}/src`)) {
      const source = await Bun.file(`${policy.directory}/src/${relativePath}`).text();
      for (const match of source.matchAll(sourceImportPattern)) {
        const specifier = match[2];
        if (specifier === undefined) continue;
        expect(specifier.includes("/src/"), `${policy.packageName}: ${specifier}`).toBe(false);
        const dependency = workspacePackageName(specifier);
        if (dependency === undefined) continue;
        expect(policy.allowed.has(dependency), `${policy.packageName} -> ${dependency}`).toBe(true);
        expect(manifest, `${policy.packageName} must declare ${dependency}`).toContain(
          `"${dependency}"`,
        );
      }
    }
  }
});
