import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useRef } from "react";
import { useCreateChat } from "../client/use-create-chat.ts";

/** Root route that durably creates a conversation before canonical navigation. */
export const Route = createFileRoute("/")({
  ssr: false,
  component: NewChatRedirect,
});

function NewChatRedirect() {
  const started = useRef(false);
  const { create, creating, error } = useCreateChat();

  useEffect(() => {
    if (started.current) return;
    started.current = true;
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
            started.current = false;
            create();
          }}
        >
          Try again
        </button>
      )}
    </main>
  );
}
