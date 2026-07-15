const logger = require('../utils/logger');
const { PermanentError } = require('../utils/errors');

/**
 * Simulates dispatch of a notification to an external gateway.
 * Supports deterministic failure injection via payload parameters for testing.
 * 
 * @param {object} event The NotificationEvent object
 * @param {number} retryCount The current retry attempt count
 * @returns {Promise<boolean>} Resolves to true on success, throws an Error on failure.
 */
async function mockDispatchNotification(event, retryCount = 0) {
  const { event_id, type, recipient, payload } = event;
  logger.info('Simulating external notification dispatch', { event_id, type, recipient, retryCount });

  // Simulate network latency of the API call (100ms)
  await new Promise(resolve => setTimeout(resolve, 100));

  if (payload) {
    // 1. Permanent failure flag -> throws PermanentError
    if (payload.should_fail === true) {
      logger.warn('Mock dispatch: injected permanent failure triggered', { event_id });
      throw new PermanentError('Gateway rejection: Invalid recipient address (simulated permanent failure)');
    }

    // 2. Transient failure flag -> throws standard Error (transient)
    if (payload.fail_attempts && retryCount < payload.fail_attempts) {
      logger.warn('Mock dispatch: injected transient failure triggered', { event_id, retryCount, fail_attempts: payload.fail_attempts });
      throw new Error(`Gateway timeout: connection reset (simulated transient failure, retry ${retryCount}/${payload.fail_attempts})`);
    }
  }

  logger.info('Mock dispatch: notification dispatched successfully', { event_id });
  return true;
}

module.exports = {
  mockDispatchNotification
};
