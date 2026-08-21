import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Test from "alchemy/Test/Bun";
import { expect, test as bunTest } from "bun:test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import Stack from "../alchemy.run.ts";
import {
  AgentRpcs,
  AgentStreamEvent,
  AgentContext,
  SessionId,
} from "../src/shared/agent-protocol.ts";

const live = process.env.RUN_LIVE_E2E === "1";

if (!live) {
  bunTest.skip("live Cloudflare E2E (set RUN_LIVE_E2E=1 or run bun run test:e2e)", () => {});
} else {
  const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
    dev: false,
  });

  const stack = beforeAll(deploy(Stack));
  afterAll(destroy(Stack));

  const protocol = stack.pipe(
    Effect.map(({ websiteUrl }) =>
      RpcClient.layerProtocolHttp({
        url: new URL("/rpc", websiteUrl).toString(),
      }),
    ),
    Layer.unwrap,
    Layer.provide(FetchHttpClient.layer),
    Layer.provide(RpcSerialization.layerNdjson),
  );

  test(
    "verifies messages, context, tools, and compaction through shared Effect RPC",
    Effect.gen(function* () {
      const { websiteUrl } = yield* stack;
      const ready = yield* Test.getWhenReady(websiteUrl);
      expect(ready.status).toBe(200);
      const client = yield* RpcClient.make(AgentRpcs);
      const sessionId = SessionId.make(`live-${crypto.randomUUID()}`);
      const verificationCode = `ORCHID-${crypto.randomUUID().slice(0, 8)}`;

      const context = AgentContext.make({
        systemPrompt:
          "You are a concise verification assistant. Follow direct instructions exactly.",
        memory: `The verification code is ${verificationCode}.`,
      });
      const contextSnapshot = yield* client.updateContext({
        sessionId,
        context,
      });
      expect(contextSnapshot.context).toEqual(context);

      const contextEvents = Array.from(
        yield* client
          .sendMessage({
            sessionId,
            prompt: "Reply with only the verification code from memory.",
          })
          .pipe(Stream.runCollect),
      );
      const contextAnswer = contextEvents
        .filter(AgentStreamEvent.guards.TextDelta)
        .map((event) => event.delta)
        .join("");
      expect(contextAnswer).toContain(verificationCode);

      const toolEvents = Array.from(
        yield* client
          .sendMessage({
            sessionId,
            prompt: "Call lookup_project_fact with topic protocol, then answer using its result.",
          })
          .pipe(Stream.runCollect),
      );
      expect(toolEvents.some(AgentStreamEvent.guards.ToolCall)).toBe(true);
      const toolResult = toolEvents.find(AgentStreamEvent.guards.ToolResult);
      expect(toolResult?.isFailure).toBe(false);
      expect(JSON.stringify(toolResult?.output)).toContain("Effect RpcGroup");

      yield* client
        .sendMessage({ sessionId, prompt: "Reply with: acknowledged" })
        .pipe(Stream.runDrain);

      const before = yield* client.getSession({ sessionId });
      expect(before.messages.length).toBeGreaterThanOrEqual(8);
      const compaction = yield* client.compactSession({ sessionId });
      expect(compaction.compacted).toBe(true);

      const after = yield* client.getSession({ sessionId });
      expect(after.messages).toEqual(before.messages);
      expect(after.compactions.length).toBeGreaterThan(0);
      expect(after.stats.modelMessageCount).toBeLessThan(after.stats.rawMessageCount);
    }).pipe(Effect.provide(protocol)),
    { timeout: 360_000 },
  );
}
