import { useAtomRefresh } from "@effect/atom-react";
import { useNavigate } from "@tanstack/react-router";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import { useCallback, useState } from "react";
import { SessionId } from "@chemistry/contracts/agent-protocol";
import { chatListAtom, createDurableChat } from "./agent-client.ts";

/** Create durable chats and navigate only after catalog persistence succeeds. */
export function useCreateChat() {
  const navigate = useNavigate();
  const refreshChats = useAtomRefresh(chatListAtom);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const create = useCallback(() => {
    if (creating) return;
    const sessionId = SessionId.make(`chat-${crypto.randomUUID()}`);
    setCreating(true);
    setError(null);
    void Effect.runPromise(createDurableChat(sessionId).pipe(Effect.result)).then((result) => {
      setCreating(false);
      if (Result.isFailure(result)) {
        setError("Couldn’t create a new chat. Please try again.");
        return;
      }
      refreshChats();
      void navigate({ to: "/chat/$chatId", params: { chatId: sessionId } });
    });
  }, [creating, navigate, refreshChats]);

  return { create, creating, error } as const;
}
