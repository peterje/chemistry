import { expect, test, type Page } from "@playwright/test";

const openHome = async (page: Page): Promise<void> => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Starter" })).toBeVisible();
};

test("the demo home page renders and increments", async ({ page }) => {
  await openHome(page);
  await expect(page.getByText("Count 0")).toBeVisible();
  await expect(page.locator("[data-phase='idle']")).toHaveText("Idle");
  await page.getByRole("button", { name: "Increment" }).click();
  await expect(page.getByText("Count 1")).toBeVisible();
  await expect(page.locator("[data-phase='ready']")).toHaveText("Ready");
});
