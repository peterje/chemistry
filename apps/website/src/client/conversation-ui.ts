const NEAR_BOTTOM_PX = 96;

/** Keep the last successful snapshot so chat switches do not flash a loading shell. */
export const rememberSessionSnapshot = <Snapshot>(
  cache: Map<string, Snapshot>,
  sessionId: string,
  snapshot: Snapshot | undefined,
): Snapshot | undefined => {
  if (snapshot !== undefined) cache.set(sessionId, snapshot);
  return snapshot ?? cache.get(sessionId);
};

/** True when the conversation scroller is pinned close enough to the latest message. */
export const isNearBottom = (
  box: Readonly<{ scrollHeight: number; scrollTop: number; clientHeight: number }>,
  thresholdPx: number = NEAR_BOTTOM_PX,
): boolean => box.scrollHeight - box.scrollTop - box.clientHeight <= thresholdPx;
