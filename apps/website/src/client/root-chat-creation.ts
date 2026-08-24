let rootChatCreationLocked = false;

/** Take the root-route chat creation lock, returning false when it is already held. */
export const acquireRootChatCreation = (): boolean => {
  if (rootChatCreationLocked) return false;
  rootChatCreationLocked = true;
  return true;
};

/** Release the root-route lock after a chat page has mounted or creation failed. */
export const releaseRootChatCreation = (): void => {
  rootChatCreationLocked = false;
};
