import { TranscriptPart, type TranscriptMessage } from "../../shared/agent-protocol.ts";

/** Render one role-labelled durable transcript message. */
export function TranscriptRow({ message }: Readonly<{ message: TranscriptMessage }>) {
  return (
    <li className={`message message-${message.role}`}>
      <span className="message-role">{message.role}</span>
      <div className="message-body">
        {message.parts.map((part, index) =>
          TranscriptPart.match(part, {
            Text: ({ text }) => <p key={`${message.id}-text-${index}`}>{text}</p>,
            ToolCall: ({ name, input }) => (
              <div className="tool-part" key={`${message.id}-call-${index}`}>
                <b>CALL · {name}</b>
                <code>{JSON.stringify(input)}</code>
              </div>
            ),
            ToolResult: ({ name, output, isFailure }) => (
              <div className="tool-part" key={`${message.id}-result-${index}`}>
                <b>
                  {isFailure ? "FAILED" : "RESULT"} · {name}
                </b>
                <code>{JSON.stringify(output)}</code>
              </div>
            ),
          }),
        )}
      </div>
    </li>
  );
}
