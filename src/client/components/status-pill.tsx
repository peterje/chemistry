import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import type { SessionSnapshot } from "../../shared/agent-protocol.ts";

/** Render the loading, durable, refreshing, or failed query state. */
export function StatusPill({
  result,
}: Readonly<{ result: AsyncResult.AsyncResult<SessionSnapshot, unknown> }>) {
  const label = AsyncResult.isSuccess(result)
    ? result.waiting
      ? "REFRESHING"
      : "DURABLE"
    : AsyncResult.isFailure(result)
      ? "RPC ERROR"
      : "CONNECTING";
  const className = `status status-${label.toLowerCase().replace(" ", "-")}`;
  return <span className={className}>{label}</span>;
}
