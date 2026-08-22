import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { AgentProtocolError, RuntimeClientFrame, RuntimeServerFrame } from "./agent-protocol.ts";

/** Maximum UTF-8 size accepted for one WebSocket protocol frame. */
export const MAX_RUNTIME_FRAME_BYTES = 64 * 1_024;

const encoder = new TextEncoder();

const protocolError = (code: string, message: string) => new AgentProtocolError({ code, message });

/** Decode and parse one bounded browser-to-runtime JSON frame. */
export const decodeRuntimeClientFrame = Effect.fn("RuntimeProtocol.decodeClientFrame")(function* (
  encoded: string,
) {
  if (encoder.encode(encoded).byteLength > MAX_RUNTIME_FRAME_BYTES) {
    return yield* protocolError(
      "frame-too-large",
      `Runtime frame exceeds ${MAX_RUNTIME_FRAME_BYTES} bytes`,
    );
  }
  const json = yield* Effect.try({
    try: () => JSON.parse(encoded),
    catch: () => protocolError("invalid-json", "Runtime frame is not valid JSON"),
  });
  return yield* Schema.decodeUnknownEffect(RuntimeClientFrame)(json).pipe(
    Effect.mapError(() =>
      protocolError("invalid-frame", "Runtime frame does not match the shared schema"),
    ),
  );
});

/** Encode one browser-to-runtime frame after checking the shared schema and size bound. */
export const encodeRuntimeClientFrame = Effect.fn("RuntimeProtocol.encodeClientFrame")(function* (
  frame: RuntimeClientFrame,
) {
  const encodedValue = yield* Schema.encodeUnknownEffect(RuntimeClientFrame)(frame).pipe(
    Effect.mapError(() => protocolError("encode-failed", "Runtime frame could not be encoded")),
  );
  const encoded = JSON.stringify(encodedValue);
  if (encoder.encode(encoded).byteLength > MAX_RUNTIME_FRAME_BYTES) {
    return yield* protocolError(
      "frame-too-large",
      `Encoded runtime frame exceeds ${MAX_RUNTIME_FRAME_BYTES} bytes`,
    );
  }
  return encoded;
});

/** Encode one runtime-to-browser frame after checking the shared schema and size bound. */
export const encodeRuntimeServerFrame = Effect.fn("RuntimeProtocol.encodeServerFrame")(function* (
  frame: RuntimeServerFrame,
) {
  const encodedValue = yield* Schema.encodeUnknownEffect(RuntimeServerFrame)(frame).pipe(
    Effect.mapError(() => protocolError("encode-failed", "Runtime frame could not be encoded")),
  );
  const encoded = JSON.stringify(encodedValue);
  if (encoder.encode(encoded).byteLength > MAX_RUNTIME_FRAME_BYTES) {
    return yield* protocolError(
      "frame-too-large",
      `Encoded runtime frame exceeds ${MAX_RUNTIME_FRAME_BYTES} bytes`,
    );
  }
  return encoded;
});

/** Decode one bounded runtime-to-browser JSON frame. */
export const decodeRuntimeServerFrame = Effect.fn("RuntimeProtocol.decodeServerFrame")(function* (
  encoded: string,
) {
  if (encoder.encode(encoded).byteLength > MAX_RUNTIME_FRAME_BYTES) {
    return yield* protocolError(
      "frame-too-large",
      `Runtime frame exceeds ${MAX_RUNTIME_FRAME_BYTES} bytes`,
    );
  }
  const json = yield* Effect.try({
    try: () => JSON.parse(encoded),
    catch: () => protocolError("invalid-json", "Runtime frame is not valid JSON"),
  });
  return yield* Schema.decodeUnknownEffect(RuntimeServerFrame)(json).pipe(
    Effect.mapError(() =>
      protocolError("invalid-frame", "Runtime frame does not match the shared schema"),
    ),
  );
});
