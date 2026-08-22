import { TranscriptPart, type TranscriptMessage } from "@chemistry/contracts/agent-protocol";
import { MarkdownMessage } from "./markdown-message.tsx";

/** Render one durable transcript message in the user/assistant chat language. */
export function TranscriptRow({ message }: Readonly<{ message: TranscriptMessage }>) {
  const assistant = message.role === "assistant";
  const user = message.role === "user";

  return (
    <li className={`message message-${message.role}`}>
      {assistant && (
        <span className="assistant-avatar" aria-label="Chemistry">
          C
        </span>
      )}
      <div className="message-body">
        {message.parts.map((part, index) =>
          TranscriptPart.match(part, {
            Text: ({ text }) =>
              assistant ? (
                <MarkdownMessage key={`${message.id}-text-${index}`}>{text}</MarkdownMessage>
              ) : (
                <p
                  className={user ? "user-text" : "message-text"}
                  key={`${message.id}-text-${index}`}
                >
                  {text}
                </p>
              ),
            ToolCall: ({ name, input }) => (
              <details className="tool-activity" key={`${message.id}-call-${index}`}>
                <summary>
                  <span className="tool-status" aria-hidden="true" />
                  Used {name}
                </summary>
                <pre>{JSON.stringify(input, null, 2)}</pre>
              </details>
            ),
            ToolResult: ({ name, output, isFailure }) => (
              <details
                className={`tool-activity${isFailure ? " tool-failed" : ""}`}
                key={`${message.id}-result-${index}`}
              >
                <summary>
                  <span className="tool-status" aria-hidden="true" />
                  {isFailure ? "Failed" : "Result from"} {name}
                </summary>
                <pre>{JSON.stringify(output, null, 2)}</pre>
              </details>
            ),
          }),
        )}
      </div>
    </li>
  );
}
