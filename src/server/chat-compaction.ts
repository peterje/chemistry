import * as Prompt from "effect/unstable/ai/Prompt";
import {
  TranscriptPart,
  type Compaction,
  type TranscriptMessage,
} from "../shared/agent-protocol.ts";
import type { StoredSession } from "./session-store.ts";

/** Input required to summarize the next eligible raw transcript range. */
export interface CompactionPlan {
  /** First raw message covered by the resulting overlay. */
  readonly fromMessage: TranscriptMessage;
  /** Last raw message covered by the resulting overlay. */
  readonly toMessage: TranscriptMessage;
  /** Total raw messages covered by the resulting overlay. */
  readonly sourceMessageCount: number;
  /** Prompt asking the model to combine prior summary and newly eligible history. */
  readonly prompt: Prompt.Prompt;
}

const containsToolCall = (message: TranscriptMessage): boolean =>
  message.parts.some(TranscriptPart.guards.ToolCall);

const safeEndExclusive = (
  messages: ReadonlyArray<TranscriptMessage>,
  retainRecentMessages: number,
): number => {
  let endExclusive = Math.max(0, messages.length - retainRecentMessages);
  while (endExclusive > 0 && endExclusive < messages.length) {
    const previous = messages[endExclusive - 1];
    const next = messages[endExclusive];
    if (
      next?.role === "tool" ||
      previous?.role === "tool" ||
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
  messages: ReadonlyArray<TranscriptMessage>,
  overlay: Compaction | undefined,
): number => {
  if (overlay === undefined) return -1;
  return messages.findIndex((message) => message.id === overlay.toMessageId);
};

const renderPart = (part: TranscriptPart): string =>
  TranscriptPart.match(part, {
    Text: ({ text }) => text,
    ToolCall: ({ name, input }) => `[tool call ${name}] ${JSON.stringify(input)}`,
    ToolResult: ({ name, output, isFailure }) =>
      `[tool result ${name}${isFailure ? " failed" : ""}] ${JSON.stringify(output)}`,
  });

const renderMessage = (message: TranscriptMessage): string =>
  `${message.role.toUpperCase()}: ${message.parts.map(renderPart).join("\n")}`;

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
