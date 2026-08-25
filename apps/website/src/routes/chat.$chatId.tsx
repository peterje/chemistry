import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { createFileRoute, Link } from "@tanstack/react-router";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { ChatSidebar } from "../client/components/chat-sidebar.tsx";
import { MessageComposer } from "../client/components/message-composer.tsx";
import { Transcript } from "../client/components/transcript.tsx";
import { chatListAtom, createDurableChat, sessionSnapshotAtom } from "../client/agent-client.ts";
import { isNearBottom, rememberSessionSnapshot } from "../client/conversation-ui.ts";
import { releaseRootChatCreation } from "../client/root-chat-creation.ts";
import { useCreateChat } from "../client/use-create-chat.ts";
import { useResumableAgent } from "../client/use-resumable-agent.ts";
import {
  SessionId,
  type SessionId as SessionIdType,
  type SessionSnapshot,
} from "@chemistry/contracts/agent-protocol";

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

const desktopHistoryQuery = "(min-width: 761px)";

function ChatPage({ sessionId }: Readonly<{ sessionId: SessionIdType }>) {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [desktopSidebarOpen, setDesktopSidebarOpen] = useState(true);
  const [isDesktop, setIsDesktop] = useState(true);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  const conversationScroll = useRef<HTMLDivElement>(null);
  const snapshotCache = useRef(new Map<string, SessionSnapshot>());
  const pinnedToBottom = useRef(true);
  const lastSessionId = useRef(sessionId);
  const snapshotAtom = useMemo(() => sessionSnapshotAtom(sessionId), [sessionId]);
  const snapshotResult = useAtomValue(snapshotAtom);
  const refreshSnapshot = useAtomRefresh(snapshotAtom);
  const refreshChats = useAtomRefresh(chatListAtom);
  const chatListResult = useAtomValue(chatListAtom);
  const chatsResultRef = useRef(chatListResult);
  const runtime = useResumableAgent(sessionId);
  const createChat = useCreateChat();
  const liveSnapshot = AsyncResult.isSuccess(snapshotResult) ? snapshotResult.value : undefined;
  const snapshot = rememberSessionSnapshot(snapshotCache.current, sessionId, liveSnapshot);
  const snapshotFailed = AsyncResult.isFailure(snapshotResult) && snapshot === undefined;
  const historyOpen = isDesktop ? desktopSidebarOpen : sidebarOpen;
  const activeTitle = AsyncResult.isSuccess(chatListResult)
    ? chatListResult.value.chats.find((chat) => chat.sessionId === sessionId)?.title
    : undefined;

  useEffect(() => {
    releaseRootChatCreation();
  }, []);

  useEffect(() => {
    chatsResultRef.current = chatListResult;
  }, [chatListResult]);

  useEffect(() => {
    let active = true;
    const listed = chatsResultRef.current;
    const alreadyListed =
      AsyncResult.isSuccess(listed) &&
      listed.value.chats.some((chat) => chat.sessionId === sessionId);
    void Effect.runPromise(createDurableChat(sessionId).pipe(Effect.result)).then((result) => {
      if (!active) return;
      if (Result.isFailure(result)) {
        setCatalogError("Chat history is temporarily unavailable.");
        return;
      }
      setCatalogError(null);
      if (!alreadyListed) refreshChats();
    });
    return () => {
      active = false;
    };
  }, [refreshChats, sessionId]);

  useEffect(() => {
    if (runtime.snapshot.checkpoint === "admitted") refreshChats();
  }, [refreshChats, runtime.snapshot.checkpoint, runtime.snapshot.operationId]);

  useEffect(() => {
    const media = window.matchMedia(desktopHistoryQuery);
    const sync = () => setIsDesktop(media.matches);
    sync();
    media.addEventListener("change", sync);
    return () => media.removeEventListener("change", sync);
  }, []);

  useEffect(() => {
    const container = conversationScroll.current;
    if (container === null) return;
    const updatePin = () => {
      pinnedToBottom.current = isNearBottom(container);
    };
    updatePin();
    container.addEventListener("scroll", updatePin, { passive: true });
    return () => container.removeEventListener("scroll", updatePin);
  }, [sessionId]);

  useLayoutEffect(() => {
    const container = conversationScroll.current;
    if (container === null) return;
    const sessionChanged = lastSessionId.current !== sessionId;
    lastSessionId.current = sessionId;
    if (sessionChanged) pinnedToBottom.current = true;
    if (sessionChanged || pinnedToBottom.current) container.scrollTop = container.scrollHeight;
  }, [sessionId, runtime.snapshot.lastSequence, snapshot?.chat.metadata.length]);

  return (
    <main className={desktopSidebarOpen ? "chat-app" : "chat-app sidebar-collapsed"}>
      <ChatSidebar
        activeSessionId={sessionId}
        open={sidebarOpen}
        creating={createChat.creating}
        createError={createChat.error}
        onClose={() => {
          setSidebarOpen(false);
        }}
        onCollapse={() => {
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
            aria-expanded={historyOpen}
            aria-label={historyOpen ? "Close conversation history" : "Open conversation history"}
            onClick={() => {
              if (isDesktop) {
                setDesktopSidebarOpen((open) => !open);
                setSidebarOpen(false);
                return;
              }
              setSidebarOpen((open) => !open);
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
            key={sessionId}
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
          onSubmitted={() => {
            pinnedToBottom.current = true;
          }}
        />
      </section>
    </main>
  );
}
