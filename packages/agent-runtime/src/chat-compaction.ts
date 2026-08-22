import * as Prompt from "effect/unstable/ai/Prompt";
import type { ChatMessage, Compaction } from "@chemistry/contracts/agent-protocol";
import type { StoredSession } from "./session-store.ts";

/** Input required to summarize the next eligible raw transcript range. */
export interface CompactionPlan {
  /** First raw message covered by the resulting overlay. */
  readonly fromMessage: ChatMessage;
  /** Last raw message covered by the resulting overlay. */
  readonly toMessage: ChatMessage;
  /** Total raw messages covered by the resulting overlay. */
  readonly sourceMessageCount: number;
  /** Prompt asking the model to combine prior summary and newly eligible history. */
  readonly prompt: Prompt.Prompt;
}

const containsToolCall = (entry: ChatMessage): boolean =>
  entry.message.role === "assistant" &&
  entry.message.content.some((part) => part.type === "tool-call");

const safeEndExclusive = (
  messages: ReadonlyArray<ChatMessage>,
  retainRecentMessages: number,
): number => {
  let endExclusive = Math.max(0, messages.length - retainRecentMessages);
  while (endExclusive > 0 && endExclusive < messages.length) {
    const previous = messages[endExclusive - 1];
    const next = messages[endExclusive];
    if (
      next?.message.role === "tool" ||
      previous?.message.role === "tool" ||
      (previous !== undefined && containsToolCall(previous))
    ) {
      endExclusive -= 1;
      continue;
    }
    break;
  }
  return endExclusive;
};

const overlayEndIndex = (
  messages: ReadonlyArray<ChatMessage>,
  overlay: Compaction | undefined,
): number => {
  if (overlay === undefined) return -1;
  return messages.findIndex((message) => message.id === overlay.toMessageId);
};

const renderPart = (part: Prompt.Part): string => {
  switch (part.type) {
    case "text":
    case "reasoning":
      return part.text;
    case "file":
      return `[file ${part.fileName ?? part.mediaType}]`;
    case "tool-call":
      return `[tool call ${part.name}] ${JSON.stringify(part.params)}`;
    case "tool-result":
      return `[tool result ${part.name}${part.isFailure ? " failed" : ""}] ${JSON.stringify(part.result)}`;
    case "tool-approval-request":
      return `[tool approval requested ${part.toolCallId}]`;
    case "tool-approval-response":
      return `[tool approval ${part.approved ? "granted" : "denied"}] ${part.reason ?? ""}`;
  }
};

const renderMessage = (entry: ChatMessage): string => {
  const { message } = entry;
  const content =
    message.role === "system"
      ? message.content
      : message.content.map((part) => renderPart(part)).join("\n");
  return `${message.role.toUpperCase()}: ${content}`;
};

/** Build the next safe, non-overlapping compaction plan, if history is eligible. */
export const buildCompactionPlan = (
  session: StoredSession,
  retainRecentMessages: number,
): CompactionPlan | undefined => {
  const endExclusive = safeEndExclusive(session.messages, Math.max(1, retainRecentMessages));
  if (endExclusive < 2) return undefined;

  const latest = session.compactions.at(-1);
  const previousEnd = overlayEndIndex(session.messages, latest);
  if (endExclusive - 1 <= previousEnd) return undefined;

  const fromMessage = session.messages[0];
  const toMessage = session.messages[endExclusive - 1];
  if (fromMessage === undefined || toMessage === undefined) return undefined;

  const newlyEligible = session.messages.slice(previousEnd + 1, endExclusive);
  const previousSummary = latest?.summary;
  const transcript = newlyEligible.map(renderMessage).join("\n\n");
  const summaryInput =
    previousSummary === undefined
      ? transcript
      : `<previous-summary>\n${previousSummary}\n</previous-summary>\n\n<new-transcript>\n${transcript}\n</new-transcript>`;

  return {
    fromMessage,
    toMessage,
    sourceMessageCount: endExclusive,
    prompt: Prompt.fromMessages([
      Prompt.systemMessage({
        content:
          "You compact conversation history. Return only a concise factual summary that preserves user preferences, decisions, unresolved work, and tool findings. Do not add facts.",
      }),
      Prompt.userMessage({
        content: [Prompt.textPart({ text: summaryInput })],
      }),
    ]),
  };
};
