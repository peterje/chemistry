import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { RuntimeDiagnostics } from "../src/client/components/runtime-diagnostics.tsx";
import { OperationId, StreamId } from "../src/shared/agent-protocol.ts";

test("runtime diagnostics renders replay, checkpoint, recovery, and terminal evidence", () => {
  const markup = renderToStaticMarkup(
    <RuntimeDiagnostics
      runtime={{
        status: "recovering",
        streamId: StreamId.make("diagnostic-stream"),
        operationId: OperationId.make("diagnostic-operation"),
        lastSequence: 12,
        checkpoint: "partial-persisted",
        recoveryAttempt: 2,
        terminalReason: "inference-stall",
        error: null,
        recentEvents: [],
      }}
    />,
  );
  expect(markup).toContain("recovering");
  expect(markup).toContain("diagnostic-stream");
  expect(markup).toContain("partial-persisted");
  expect(markup).toContain("attempt 2");
  expect(markup).toContain("inference-stall");
});
