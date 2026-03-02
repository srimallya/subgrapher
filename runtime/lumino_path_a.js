function createPathAExecutor(options = {}) {
  const legacyExecute = typeof options.executeLegacyLuminoChat === 'function'
    ? options.executeLegacyLuminoChat
    : null;

  if (!legacyExecute) {
    throw new Error('createPathAExecutor requires executeLegacyLuminoChat.');
  }

  async function executePathAChat(input = {}, opts = {}) {
    return legacyExecute(input, {
      ...((opts && typeof opts === 'object') ? opts : {}),
      lane: 'path_a',
    });
  }

  return {
    executePathAChat,
  };
}

module.exports = {
  createPathAExecutor,
};
