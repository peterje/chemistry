import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { MarkdownMessage } from "../src/client/components/markdown-message.tsx";

test("assistant Markdown renders semantic emphasis, lists, and tables", () => {
  const markup = renderToStaticMarkup(
    <MarkdownMessage>
      {"**Durable**\n\n- replay\n- recovery\n\n| A | B |\n|---|---|\n| 1 | 2 |"}
    </MarkdownMessage>,
  );
  expect(markup).toContain("<strong>Durable</strong>");
  expect(markup).toContain("<li>replay</li>");
  expect(markup).toContain("<table>");
});
