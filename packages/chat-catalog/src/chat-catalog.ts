import {
  ChatList,
  ChatSummary,
  ChatTitle,
  SessionId,
  type SessionId as SessionIdType,
} from "@chemistry/contracts/agent-protocol";
import * as Schema from "effect/Schema";

const DEFAULT_TITLE = ChatTitle.make("New chat");
const MAX_TITLE_LENGTH = 56;

/** Maximum number of recent conversations retained in the navigation catalog. */
export const MAX_CATALOG_CHATS = 200;

const ChatCatalogEntry = Schema.Struct({
  sessionId: SessionId,
  title: ChatTitle,
  titleStatus: Schema.Literals(["placeholder", "first-prompt"]),
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
}).annotate({ identifier: "ChatCatalogEntry" });

interface ChatCatalogEntry extends Schema.Schema.Type<typeof ChatCatalogEntry> {}

/** Legacy catalog state written before title provenance was persisted explicitly. */
export const LegacyChatCatalogStateV1 = Schema.Struct({
  version: Schema.Literal(1),
  chats: Schema.Array(ChatSummary).check(Schema.isMaxLength(MAX_CATALOG_CHATS)),
}).annotate({ identifier: "LegacyChatCatalogStateV1" });

/** Decoded legacy catalog state. */
export interface LegacyChatCatalogStateV1 extends Schema.Schema.Type<
  typeof LegacyChatCatalogStateV1
> {}

/** Versioned server-persisted state for the bounded conversation catalog. */
export const ChatCatalogState = Schema.Struct({
  version: Schema.Literal(2),
  chats: Schema.Array(ChatCatalogEntry).check(Schema.isMaxLength(MAX_CATALOG_CHATS)),
}).annotate({ identifier: "ChatCatalogState" });

/** Decoded durable conversation-catalog state. */
export interface ChatCatalogState extends Schema.Schema.Type<typeof ChatCatalogState> {}

/** Construct an empty conversation catalog. */
export const emptyChatCatalog = (): ChatCatalogState =>
  ChatCatalogState.make({ version: 2, chats: [] });

const orderedEntries = (chats: ReadonlyArray<ChatCatalogEntry>): ReadonlyArray<ChatCatalogEntry> =>
  [...chats].sort(
    (left, right) =>
      right.updatedAt - left.updatedAt || left.sessionId.localeCompare(right.sessionId),
  );

const bounded = (chats: ReadonlyArray<ChatCatalogEntry>): ReadonlyArray<ChatCatalogEntry> =>
  orderedEntries(chats).slice(0, MAX_CATALOG_CHATS);

const summaryOf = (entry: ChatCatalogEntry): ChatSummary =>
  ChatSummary.make({
    sessionId: entry.sessionId,
    title: entry.title,
    createdAt: entry.createdAt,
    updatedAt: entry.updatedAt,
  });

/** Migrate legacy summaries without risking a later rewrite of an existing first-message title. */
export const migrateChatCatalogStateV1 = (legacy: LegacyChatCatalogStateV1): ChatCatalogState =>
  ChatCatalogState.make({
    version: 2,
    chats: legacy.chats.map((chat) =>
      ChatCatalogEntry.make({
        ...chat,
        titleStatus: "first-prompt",
      }),
    ),
  });

/** Derive a compact stable sidebar title from the first user prompt. */
export const titleFromFirstPrompt = (prompt: string): typeof ChatTitle.Type => {
  const normalized = prompt.trim().replace(/\s+/g, " ");
  if (normalized.length === 0) return DEFAULT_TITLE;
  if (normalized.length <= MAX_TITLE_LENGTH) return ChatTitle.make(normalized);
  const prefix = normalized.slice(0, MAX_TITLE_LENGTH - 1).trimEnd();
  const lastSpace = prefix.lastIndexOf(" ");
  const clipped = lastSpace >= 28 ? prefix.slice(0, lastSpace) : prefix;
  return ChatTitle.make(`${clipped}…`);
};

/** Idempotently add a new empty conversation to the durable catalog. */
export const createChat = (
  state: ChatCatalogState,
  sessionId: SessionIdType,
  now: number,
): readonly [ChatCatalogState, ChatSummary] => {
  const existing = state.chats.find((chat) => chat.sessionId === sessionId);
  if (existing !== undefined) return [state, summaryOf(existing)];
  const created = ChatCatalogEntry.make({
    sessionId,
    title: DEFAULT_TITLE,
    titleStatus: "placeholder",
    createdAt: now,
    updatedAt: now,
  });
  return [
    ChatCatalogState.make({ version: 2, chats: bounded([...state.chats, created]) }),
    summaryOf(created),
  ];
};

/** Record accepted user activity and set the immutable first-prompt title exactly once. */
export const touchChat = (
  state: ChatCatalogState,
  sessionId: SessionIdType,
  prompt: string,
  now: number,
): readonly [ChatCatalogState, ChatSummary] => {
  const existing = state.chats.find((chat) => chat.sessionId === sessionId);
  const touched = ChatCatalogEntry.make({
    sessionId,
    title:
      existing === undefined || existing.titleStatus === "placeholder"
        ? titleFromFirstPrompt(prompt)
        : existing.title,
    titleStatus: "first-prompt",
    createdAt: existing?.createdAt ?? now,
    updatedAt: now,
  });
  const others = state.chats.filter((chat) => chat.sessionId !== sessionId);
  return [
    ChatCatalogState.make({ version: 2, chats: bounded([...others, touched]) }),
    summaryOf(touched),
  ];
};

/** Project durable catalog state into its public recency-ordered list. */
export const listChats = (state: ChatCatalogState): ChatList =>
  ChatList.make({ chats: orderedEntries(state.chats).map(summaryOf) });
