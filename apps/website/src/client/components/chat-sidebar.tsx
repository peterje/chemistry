import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { Link } from "@tanstack/react-router";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { chatListAtom } from "../agent-client.ts";
import type { SessionId } from "@chemistry/contracts/agent-protocol";

/** Durable conversation navigation for desktop and mobile chat layouts. */
export function ChatSidebar({
  activeSessionId,
  open,
  creating,
  createError,
  onClose,
  onCollapse,
  onCreate,
}: Readonly<{
  activeSessionId: SessionId;
  open: boolean;
  creating: boolean;
  createError: string | null;
  onClose: () => void;
  onCollapse: () => void;
  onCreate: () => void;
}>) {
  const result = useAtomValue(chatListAtom);
  const refreshChats = useAtomRefresh(chatListAtom);
  const chats = AsyncResult.isSuccess(result) ? result.value.chats : [];

  return (
    <>
      {open && (
        <button
          type="button"
          className="sidebar-scrim"
          aria-label="Close conversation history"
          onClick={onClose}
        />
      )}
      <aside className={`chat-sidebar${open ? " chat-sidebar-open" : ""}`} aria-label="Chats">
        <div className="sidebar-header">
          <Link to="/" className="wordmark" onClick={onClose}>
            Chemistry
          </Link>
          <button
            type="button"
            className="sidebar-close"
            onClick={onCollapse}
            aria-label="Close chats"
          >
            ×
          </button>
        </div>
        <button type="button" className="new-chat-button" onClick={onCreate} disabled={creating}>
          <span aria-hidden="true">＋</span>
          {creating ? "Creating…" : "New chat"}
        </button>
        {createError !== null && <p className="sidebar-error">{createError}</p>}
        <nav className="chat-history" aria-label="Conversation history">
          <p className="history-label">Recent</p>
          {AsyncResult.isFailure(result) ? (
            <div className="history-error" role="alert">
              <p>Couldn’t load conversation history.</p>
              <button type="button" onClick={refreshChats}>
                Try again
              </button>
            </div>
          ) : AsyncResult.isInitial(result) || AsyncResult.isWaiting(result) ? (
            <p className="history-state">Loading chats…</p>
          ) : chats.length === 0 ? (
            <p className="history-state">No conversations yet.</p>
          ) : (
            <ul>
              {chats.map((chat) => (
                <li key={chat.sessionId}>
                  <Link
                    to="/chat/$chatId"
                    params={{ chatId: chat.sessionId }}
                    className={
                      chat.sessionId === activeSessionId ? "chat-link active" : "chat-link"
                    }
                    onClick={onClose}
                  >
                    {chat.title}
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </nav>
        <div className="sidebar-footer">
          <span className="model-dot" aria-hidden="true" />
          <span>GLM-5.2 · Workers AI</span>
        </div>
      </aside>
    </>
  );
}
