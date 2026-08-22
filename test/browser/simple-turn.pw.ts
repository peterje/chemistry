import { expect, test, type WebSocket as PlaywrightWebSocket } from "@playwright/test";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

interface ObservedFrame {
  readonly direction: "received" | "sent";
  readonly payload: string;
}

const observeFrames = (socket: PlaywrightWebSocket, frames: Array<ObservedFrame>): void => {
  socket.on("framereceived", ({ payload }) => {
    const encoded = Schema.decodeUnknownResult(Schema.String)(payload);
    if (Result.isSuccess(encoded)) {
      frames.push({ direction: "received", payload: encoded.success });
    }
  });
  socket.on("framesent", ({ payload }) => {
    const encoded = Schema.decodeUnknownResult(Schema.String)(payload);
    if (Result.isSuccess(encoded)) {
      frames.push({ direction: "sent", payload: encoded.success });
    }
  });
};

const hasFrame = (
  frames: ReadonlyArray<ObservedFrame>,
  direction: ObservedFrame["direction"],
  tag: string,
): boolean =>
  frames.some((frame) => {
    if (frame.direction !== direction) return false;
    return frame.payload.includes(`"_tag":"${tag}"`);
  });

test("a simple browser turn returns model text and ACKs every live event", async ({ page }) => {
  const frames: Array<ObservedFrame> = [];
  const browserErrors: Array<string> = [];
  page.on("websocket", (socket) => observeFrames(socket, frames));
  page.on("console", (message) => {
    if (message.type() === "error") browserErrors.push(message.text());
  });
  page.on("pageerror", (error) => browserErrors.push(error.message));

  await page.goto("/");

  const sessionId = `browser-${crypto.randomUUID()}`;
  await page.getByLabel("Durable session").fill(sessionId);
  await page.getByRole("button", { name: "Open" }).click();
  await expect(page.getByText(`One Durable Object: ${sessionId}`)).toBeVisible();
  await expect(page.locator("[data-runtime-status='live']")).toBeVisible();

  await page.getByLabel("Message the agent").fill("hi");
  await page.getByRole("button", { name: "Send via resumable WebSocket" }).click();

  await expect.poll(() => hasFrame(frames, "received", "StreamEvent")).toBe(true);
  await expect.poll(() => hasFrame(frames, "sent", "StreamAck")).toBe(true);
  await page.waitForTimeout(500);

  const protocolErrors = frames.filter(
    (frame) => frame.direction === "received" && frame.payload.includes('"_tag":"ProtocolError"'),
  );
  expect(protocolErrors).toEqual([]);

  await expect(page.locator("[data-runtime-status='completed']")).toBeVisible({
    timeout: 90_000,
  });
  await expect(page.locator(".message-user .message-body").last()).toHaveText("hi");
  await expect(page.locator(".message-assistant .message-body").last()).toContainText(/\S/);
  await expect(page.getByText("Stream ACK exceeds the events sent to this connection")).toHaveCount(
    0,
  );
  expect(browserErrors.filter((message) => /WebSocket|Stream ACK/.test(message))).toEqual([]);
});

test("the configured model completes the typed tool contract in the browser", async ({ page }) => {
  const frames: Array<ObservedFrame> = [];
  page.on("websocket", (socket) => observeFrames(socket, frames));

  await page.goto("/");
  const sessionId = `browser-tool-${crypto.randomUUID()}`;
  await page.getByLabel("Durable session").fill(sessionId);
  await page.getByRole("button", { name: "Open" }).click();
  await expect(page.locator("[data-runtime-status='live']")).toBeVisible();

  await page
    .getByLabel("Message the agent")
    .fill("Call lookup_project_fact with topic protocol, then answer using its result.");
  await page.getByRole("button", { name: "Send via resumable WebSocket" }).click();

  await expect
    .poll(
      () =>
        frames.some(
          (frame) => frame.direction === "received" && frame.payload.includes('"_tag":"ToolCall"'),
        ),
      { timeout: 90_000 },
    )
    .toBe(true);
  await expect
    .poll(
      () =>
        frames.some(
          (frame) =>
            frame.direction === "received" && frame.payload.includes('"_tag":"ToolResult"'),
        ),
      { timeout: 90_000 },
    )
    .toBe(true);
  await expect(page.locator("[data-runtime-status='completed']")).toBeVisible({
    timeout: 90_000,
  });
  await expect(page.locator(".message-assistant .message-body").last()).toContainText(/\S/);

  const protocolErrors = frames.filter(
    (frame) => frame.direction === "received" && frame.payload.includes('"_tag":"ProtocolError"'),
  );
  expect(protocolErrors).toEqual([]);
});
