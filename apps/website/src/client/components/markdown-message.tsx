import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

/** Render safe GitHub-flavored Markdown for assistant text. */
export function MarkdownMessage({ children }: Readonly<{ children: string }>) {
  return (
    <div className="markdown-content">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{children}</ReactMarkdown>
    </div>
  );
}
