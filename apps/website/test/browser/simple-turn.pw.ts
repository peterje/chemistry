import { expect, test, type Page, type WebSocket as PlaywrightWebSocket } from "@playwright/test";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

interface ObservedFrame {
  readonly direction: "received" | "sent";
  readonly payload: string;
}

const observeFrames = (socket: PlaywrightWebSocket, frames: Array<ObservedFrame>): void => {
  socket.on("framereceived", ({ payload }) => {
    const encoded = Schema.decodeUnknownResult(Schema.String)(payload);
    if (Result.isSuccess(encoded)) frames.push({ direction: "received", payload: encoded.success });
  });
  socket.on("framesent", ({ payload }) => {
    const encoded = Schema.decodeUnknownResult(Schema.String)(payload);
    if (Result.isSuccess(encoded)) frames.push({ direction: "sent", payload: encoded.success });
  });
};

const hasFrame = (
  frames: ReadonlyArray<ObservedFrame>,
  direction: ObservedFrame["direction"],
  tag: string,
): boolean =>
  frames.some(
    (frame) => frame.direction === direction && frame.payload.includes(`"_tag":"${tag}"`),
  );

const openNewChat = async (page: Page): Promise<string> => {
  await page.goto("/");
  await page.waitForURL(/\/chat\/chat-[a-f0-9-]+$/);
  await expect(page.locator("[data-runtime-status='live']")).toBeVisible();
  return new URL(page.url()).pathname;
};

const sendMessage = async (page: Page, prompt: string): Promise<void> => {
  await page.getByLabel("Message Chemistry").fill(prompt);
  await page.getByRole("button", { name: "Send message" }).click();
};

test("canonical chat routing persists history and ACKs a real model turn", async ({ page }) => {
  const frames: Array<ObservedFrame> = [];
  const browserErrors: Array<string> = [];
  page.on("websocket", (socket) => observeFrames(socket, frames));
  page.on("console", (message) => {
    if (message.type() === "error") browserErrors.push(message.text());
  });
  page.on("pageerror", (error) => browserErrors.push(error.message));

  const firstPath = await openNewChat(page);
  const app = page.locator(".chat-app");
  await expect(app).not.toHaveClass(/sidebar-collapsed/);
  await page.getByRole("button", { name: "Close conversation history" }).click();
  await expect(app).toHaveClass(/sidebar-collapsed/);
  await page.getByRole("button", { name: "Open conversation history" }).click();
  await expect(app).not.toHaveClass(/sidebar-collapsed/);
  await page.getByRole("button", { name: "Close chats" }).click();
  await expect(app).toHaveClass(/sidebar-collapsed/);
  await expect
    .poll(async () => (await page.locator(".chat-sidebar").boundingBox())?.x ?? 0)
    .toBeLessThan(0);
  await page.getByRole("button", { name: "Open conversation history" }).click();
  await expect(app).not.toHaveClass(/sidebar-collapsed/);

  await sendMessage(page, "hi");

  await expect.poll(() => hasFrame(frames, "received", "StreamEvent")).toBe(true);
  await expect.poll(() => hasFrame(frames, "sent", "StreamAck")).toBe(true);
  await expect(page.locator("[data-runtime-status='completed']")).toBeVisible({ timeout: 90_000 });
  await expect(page.locator(".message-user .message-body").last()).toHaveText("hi");
  const assistant = page.locator(".message-assistant .markdown-content").last();
  await expect(assistant).toContainText(/\S/);
  const assistantText = await assistant.innerText();

  expect(
    frames.filter(
      (frame) => frame.direction === "received" && frame.payload.includes('"_tag":"ProtocolError"'),
    ),
  ).toEqual([]);
  expect(browserErrors.filter((message) => /WebSocket|Stream ACK/.test(message))).toEqual([]);

  frames.length = 0;
  await page.reload();
  await expect(page).toHaveURL(firstPath);
  await expect(page.locator("[data-runtime-status='completed']")).toBeVisible({ timeout: 30_000 });
  expect(
    frames.filter(
      (frame) => frame.direction === "received" && frame.payload.includes('"_tag":"StreamEvent"'),
    ),
  ).toEqual([]);
  await expect(page.locator(".message-user .message-body").last()).toHaveText("hi");
  await expect(page.locator(".message-assistant .markdown-content").last()).toHaveText(
    assistantText,
  );
  await expect(page.locator(".chat-history .chat-link").first()).toHaveText("hi");

  await page.locator(".header-new-chat").click();
  await page.waitForURL((url) => url.pathname !== firstPath && url.pathname.startsWith("/chat/"));
  const secondPath = new URL(page.url()).pathname;
  expect(secondPath).not.toBe(firstPath);
  await expect(page.locator("[data-runtime-status='live']")).toBeVisible();
  await expect(page.locator(".chat-history .chat-link").first()).toHaveText("New chat");
  await expect(page.locator(".chat-history .chat-link").nth(1)).toHaveText("hi");
  const historyCount = await page.locator(".chat-history .chat-link").count();

  await page.evaluate(() => {
    document.body.dataset.loadingFlash = "0";
    new MutationObserver(() => {
      if (document.querySelector('[aria-label="Loading conversation"]')) {
        document.body.dataset.loadingFlash = "1";
      }
    }).observe(document.body, { subtree: true, childList: true, attributes: true });
  });
  await page.locator(`.chat-history .chat-link[href="${firstPath}"]`).click();
  await expect(page).toHaveURL(firstPath);
  await expect(page.locator(".message-assistant .markdown-content").last()).toHaveText(
    assistantText,
  );
  expect(await page.locator("body").getAttribute("data-loading-flash")).toBe("0");
  await expect(page.locator(".chat-history .chat-link")).toHaveCount(historyCount);
});

test("reload reconnects to an in-flight durable stream before it completes", async ({ page }) => {
  const prompt =
    "Call lookup_project_fact with topic protocol, then explain its result in ten concise bullet points.";

  await openNewChat(page);
  await sendMessage(page, prompt);
  await expect(page.locator(".live-turn .message-user .user-text")).toHaveText(prompt, {
    timeout: 15_000,
  });
  await expect(page.locator("[data-runtime-status='completed']")).toHaveCount(0);

  await page.reload();

  await expect(page.locator(".live-turn .message-user .user-text")).toHaveText(prompt, {
    timeout: 5_000,
  });
  await expect(
    page.locator("[data-runtime-status='live'], [data-runtime-status='recovering']"),
  ).toBeVisible({ timeout: 5_000 });
  await expect(page.locator("[data-runtime-status='completed']")).toHaveCount(0);
  await expect(page.locator("[data-runtime-status='completed']")).toBeVisible({ timeout: 90_000 });
});

test("the configured model completes typed tool activity in the chat UI", async ({ page }) => {
  const frames: Array<ObservedFrame> = [];
  page.on("websocket", (socket) => observeFrames(socket, frames));

  await openNewChat(page);
  await sendMessage(
    page,
    "Call lookup_project_fact with topic protocol, then answer using its result.",
  );

  await expect.poll(() => hasFrame(frames, "received", "ToolCall"), { timeout: 90_000 }).toBe(true);
  await expect
    .poll(() => hasFrame(frames, "received", "ToolResult"), { timeout: 90_000 })
    .toBe(true);
  await expect(page.locator("[data-runtime-status='completed']")).toBeVisible({ timeout: 90_000 });
  await expect(
    page.locator(".tool-activity").filter({ hasText: /Used lookup_project_fact/ }),
  ).toBeVisible();
  await expect(
    page.locator(".tool-activity").filter({ hasText: /Result from lookup_project_fact/ }),
  ).toBeVisible();
  await expect(page.locator(".message-assistant .markdown-content").last()).toContainText(/\S/);

  expect(
    frames.filter(
      (frame) => frame.direction === "received" && frame.payload.includes('"_tag":"ProtocolError"'),
    ),
  ).toEqual([]);
});

test("mobile navigation, composer submission, and history switching remain usable", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const firstPath = await openNewChat(page);

  const sidebar = page.locator(".chat-sidebar");
  const closedBox = await sidebar.boundingBox();
  expect(closedBox?.x ?? 0).toBeLessThan(0);
  await page.getByRole("button", { name: "Open conversation history" }).click();
  await expect(sidebar).toHaveClass(/chat-sidebar-open/);
  await expect.poll(async () => (await sidebar.boundingBox())?.x ?? -1).toBeGreaterThanOrEqual(0);
  await page.getByRole("button", { name: "Close chats" }).click();
  await expect(sidebar).not.toHaveClass(/chat-sidebar-open/);

  const composer = page.getByLabel("Message Chemistry");
  await expect(composer).toBeVisible();
  const composerBox = await composer.boundingBox();
  expect(composerBox?.width ?? 391).toBeLessThan(390);

  const prompt = "hi";
  await sendMessage(page, prompt);
  await expect(
    page.locator("[data-runtime-status='completed'], [data-runtime-status='failed']"),
  ).toBeVisible({ timeout: 90_000 });
  await expect(page.locator(".message-user .message-body").last()).toHaveText(prompt);

  await page.getByRole("button", { name: "Open conversation history" }).click();
  await expect(page.locator(`.chat-link[href="${firstPath}"]`)).toHaveText(prompt);
  await sidebar.getByRole("button", { name: "New chat" }).click();
  await page.waitForURL((url) => url.pathname !== firstPath && url.pathname.startsWith("/chat/"));
  await expect(sidebar).toHaveClass(/chat-sidebar-open/);
  await page.locator(`.chat-link[href="${firstPath}"]`).click();
  await expect(page).toHaveURL(firstPath);
  await expect(page.locator(".message-user .message-body").last()).toHaveText(prompt);
});

test("transcript and history queries render recoverable network error states", async ({ page }) => {
  await page.route(/\/rpc\/?$/, (route) => route.abort("failed"));
  await page.goto(`/chat/query-failure-${crypto.randomUUID()}`);

  await expect(page.getByText("We couldn’t load this conversation.")).toBeVisible();
  await expect(page.getByText("Couldn’t load conversation history.")).toBeVisible();
  await expect(page.getByRole("button", { name: "Try again" })).toHaveCount(2);
});

test("a follow-up turn keeps the previous user message visible", async ({ page }) => {
  await openNewChat(page);
  await sendMessage(page, "hi");
  await expect(page.locator("[data-runtime-status='completed']")).toBeVisible({ timeout: 90_000 });
  await sendMessage(page, "ping");
  await expect.poll(async () => page.locator(".message-user").count()).toBeGreaterThanOrEqual(2);
  await expect(page.locator(".message-user .message-body").first()).toHaveText("hi");
  await expect(page.locator(".message-user .message-body").last()).toHaveText("ping");
  await expect(page.locator("[data-runtime-status='completed']")).toBeVisible({ timeout: 90_000 });
  await expect(page.locator(".message-user")).toHaveCount(2);
});
