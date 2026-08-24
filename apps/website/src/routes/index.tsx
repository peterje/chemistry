import { createFileRoute } from "@tanstack/react-router";
import { useEffect } from "react";
import { acquireRootChatCreation, releaseRootChatCreation } from "../client/root-chat-creation.ts";
import { useCreateChat } from "../client/use-create-chat.ts";

/** Root route that durably creates a conversation before canonical navigation. */
export const Route = createFileRoute("/")({
  ssr: false,
  component: NewChatRedirect,
});

function NewChatRedirect() {
  const { create, creating, error } = useCreateChat();

  useEffect(() => {
    if (!acquireRootChatCreation()) return;
    create();
  }, [create]);

  return (
    <main className="route-loading">
      <div className="brand-mark" aria-hidden="true">
        C
      </div>
      <p>{error ?? (creating ? "Creating your chat…" : "Opening Chemistry…")}</p>
      {error !== null && (
        <button
          type="button"
          onClick={() => {
            releaseRootChatCreation();
            create();
          }}
        >
          Try again
        </button>
      )}
    </main>
  );
}
