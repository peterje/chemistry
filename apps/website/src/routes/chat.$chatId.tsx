import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { createFileRoute, Link } from "@tanstack/react-router";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { useEffect, useMemo, useRef, useState } from "react";
import { ChatSidebar } from "../client/components/chat-sidebar.tsx";
import { MessageComposer } from "../client/components/message-composer.tsx";
import { Transcript } from "../client/components/transcript.tsx";
import { chatListAtom, createDurableChat, sessionSnapshotAtom } from "../client/agent-client.ts";
import { useCreateChat } from "../client/use-create-chat.ts";
import { useResumableAgent } from "../client/use-resumable-agent.ts";
import { SessionId, type SessionId as SessionIdType } from "@chemistry/contracts/agent-protocol";

const decodeSessionId = Schema.decodeUnknownResult(SessionId);

/** Canonical route for one durable conversation. */
export const Route = createFileRoute("/chat/$chatId")({
  ssr: false,
  component: ChatRoute,
});

function ChatRoute() {
  const { chatId } = Route.useParams();
  const decoded = decodeSessionId(chatId);
  if (Result.isFailure(decoded)) {
    return (
      <main className="route-loading">
        <div className="brand-mark" aria-hidden="true">
          C
        </div>
        <h1>That chat link is invalid.</h1>
        <Link to="/">Start a new chat</Link>
      </main>
    );
  }
  return <ChatPage sessionId={decoded.success} />;
}

function ChatPage({ sessionId }: Readonly<{ sessionId: SessionIdType }>) {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [desktopSidebarOpen, setDesktopSidebarOpen] = useState(true);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  const conversationScroll = useRef<HTMLDivElement>(null);
  const snapshotAtom = useMemo(() => sessionSnapshotAtom(sessionId), [sessionId]);
  const snapshotResult = useAtomValue(snapshotAtom);
  const refreshSnapshot = useAtomRefresh(snapshotAtom);
  const refreshChats = useAtomRefresh(chatListAtom);
  const chatListResult = useAtomValue(chatListAtom);
  const runtime = useResumableAgent(sessionId);
  const createChat = useCreateChat();
  const snapshot = AsyncResult.isSuccess(snapshotResult) ? snapshotResult.value : undefined;
  const snapshotFailed = AsyncResult.isFailure(snapshotResult);
  const activeTitle = AsyncResult.isSuccess(chatListResult)
    ? chatListResult.value.chats.find((chat) => chat.sessionId === sessionId)?.title
    : undefined;

  useEffect(() => {
    let active = true;
    void Effect.runPromise(createDurableChat(sessionId).pipe(Effect.result)).then((result) => {
      if (!active) return;
      if (Result.isFailure(result)) {
        setCatalogError("Chat history is temporarily unavailable.");
        return;
      }
      setCatalogError(null);
      refreshChats();
    });
    return () => {
      active = false;
    };
  }, [refreshChats, sessionId]);

  useEffect(() => {
    if (runtime.snapshot.checkpoint === "admitted") refreshChats();
  }, [refreshChats, runtime.snapshot.checkpoint, runtime.snapshot.operationId]);

  useEffect(() => {
    const container = conversationScroll.current;
    if (container !== null) container.scrollTop = container.scrollHeight;
  }, [runtime.snapshot.lastSequence, snapshot?.chat.metadata.length]);

  return (
    <main className={desktopSidebarOpen ? "chat-app" : "chat-app sidebar-collapsed"}>
      <ChatSidebar
        activeSessionId={sessionId}
        open={sidebarOpen}
        creating={createChat.creating}
        createError={createChat.error}
        onClose={() => {
          setSidebarOpen(false);
          setDesktopSidebarOpen(false);
        }}
        onCreate={createChat.create}
      />
      <section className="chat-main" aria-label="Conversation">
        <header className="chat-header">
          <button
            type="button"
            className="sidebar-toggle"
            aria-label="Open conversation history"
            onClick={() => {
              setSidebarOpen(true);
              setDesktopSidebarOpen(true);
            }}
          >
            <span aria-hidden="true">☰</span>
          </button>
          <div>
            <strong>{activeTitle ?? "New chat"}</strong>
            <span
              className={`connection-state connection-${runtime.snapshot.status}`}
              data-runtime-status={runtime.snapshot.status}
            >
              {runtime.snapshot.status}
            </span>
          </div>
          <button
            type="button"
            className="header-new-chat"
            onClick={createChat.create}
            disabled={createChat.creating}
          >
            <span aria-hidden="true">＋</span>
            <span>New chat</span>
          </button>
        </header>
        {catalogError !== null && <p className="catalog-error">{catalogError}</p>}
        <div className="conversation-scroll" ref={conversationScroll}>
          <Transcript
            snapshot={snapshot}
            runtime={runtime.snapshot}
            failed={snapshotFailed}
            onRetry={refreshSnapshot}
          />
        </div>
        <MessageComposer
          socket={runtime.socket}
          runtime={runtime.snapshot}
          refreshSnapshot={refreshSnapshot}
        />
      </section>
    </main>
  );
}
