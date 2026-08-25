import * as Prompt from "effect/unstable/ai/Prompt";
import type { ChatMessage } from "@chemistry/contracts/agent-protocol";
import { MarkdownMessage } from "./markdown-message.tsx";

const partIdentity = (part: Prompt.Part): string => {
  switch (part.type) {
    case "text":
    case "reasoning":
      return `${part.type}:${part.text}`;
    case "file":
      return `file:${part.fileName ?? part.mediaType}`;
    case "tool-call":
    case "tool-result":
      return `${part.type}:${part.id}`;
    case "tool-approval-request":
    case "tool-approval-response":
      return `${part.type}:${part.approvalId}`;
    default: {
      const _exhaustive: never = part;
      return _exhaustive;
    }
  }
};

const keyedParts = (
  entry: ChatMessage,
): ReadonlyArray<{
  readonly key: string;
  readonly part: Prompt.Part;
}> => {
  if (entry.message.role === "system") return [];
  const occurrences = new Map<string, number>();
  return entry.message.content.map((part) => {
    const identity = partIdentity(part);
    const occurrence = occurrences.get(identity) ?? 0;
    occurrences.set(identity, occurrence + 1);
    return { key: `${entry.id}-${identity}-${occurrence}`, part };
  });
};

const renderPart = (part: Prompt.Part, key: string, assistant: boolean, user: boolean) => {
  switch (part.type) {
    case "text":
      return assistant ? (
        <MarkdownMessage key={key}>{part.text}</MarkdownMessage>
      ) : (
        <p className={user ? "user-text" : "message-text"} key={key}>
          {part.text}
        </p>
      );
    case "reasoning":
      return null;
    case "file":
      return (
        <p className="message-text" key={key}>
          File: {part.fileName ?? part.mediaType}
        </p>
      );
    case "tool-call":
      return (
        <details className="tool-activity" key={key}>
          <summary>
            <span className="tool-status" aria-hidden="true" />
            Used {part.name}
          </summary>
          <pre>{JSON.stringify(part.params, null, 2)}</pre>
        </details>
      );
    case "tool-result":
      return (
        <details className={`tool-activity${part.isFailure ? " tool-failed" : ""}`} key={key}>
          <summary>
            <span className="tool-status" aria-hidden="true" />
            {part.isFailure ? "Failed" : "Result from"} {part.name}
          </summary>
          <pre>{JSON.stringify(part.result, null, 2)}</pre>
        </details>
      );
    case "tool-approval-request":
      return (
        <p className="message-text" key={key}>
          Approval requested for tool call {part.toolCallId}
        </p>
      );
    case "tool-approval-response":
      return (
        <p className="message-text" key={key}>
          Tool request {part.approved ? "approved" : "denied"}
          {part.reason === undefined ? "" : `: ${part.reason}`}
        </p>
      );
    default: {
      const _exhaustive: never = part;
      return _exhaustive;
    }
  }
};

/** Render one durable Effect AI prompt message in the user/assistant chat language. */
export function TranscriptRow({ message: entry }: Readonly<{ message: ChatMessage }>) {
  const { message } = entry;
  const assistant = message.role === "assistant";
  const user = message.role === "user";
  const parts =
    message.role === "system"
      ? [
          <p className="message-text" key={entry.id}>
            {message.content}
          </p>,
        ]
      : keyedParts(entry).flatMap(({ key, part }) => {
          const rendered = renderPart(part, key, assistant, user);
          return rendered === null ? [] : [rendered];
        });
  if (parts.length === 0) return null;

  return (
    <li className={`message message-${message.role}`}>
      {assistant && (
        <span className="assistant-avatar" aria-label="Chemistry">
          C
        </span>
      )}
      <div className="message-body">{parts}</div>
    </li>
  );
}
