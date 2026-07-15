const { connectMQ, QUEUE_NAME } = require('../services/mq');
const db = require('../services/db');
const { mockDispatchNotification } = require('../services/notification');
const logger = require('../utils/logger');

async function startConsumer() {
  const { channel } = await connectMQ();

  // Set prefetch to process messages concurrently without overloading the consumer
  await channel.prefetch(10);

  logger.info(`Starting consumer for queue: ${QUEUE_NAME}`);

  channel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    let event;
    try {
      const contentStr = msg.content.toString();
      event = JSON.parse(contentStr);
    } catch (err) {
      logger.error('Received malformed message, discarding', { error: err.message });
      channel.ack(msg);
      return;
    }

    const { event_id, type, recipient, payload } = event;
    logger.info('Received notification event from queue', { event_id, type, recipient });

    try {
      // 1. Idempotency Check & Atomic Registration
      const regResult = await db.tryRegisterEvent(event_id);

      if (!regResult.success) {
        if (regResult.status === 'COMPLETED') {
          logger.info('Event already processed successfully. Acknowledging and skipping.', { event_id });
          channel.ack(msg);
          return;
        }

        if (regResult.status === 'PROCESSING') {
          logger.warn('Event is currently being processed by another consumer, requeueing', { event_id });
          // Sleep for 1 second before requeueing to avoid CPU/connection thrashing
          setTimeout(() => {
            try {
              channel.nack(msg, false, true); // requeue = true
            } catch (err) {
              logger.error('Failed to nack/requeue message', { event_id, error: err.message });
            }
          }, 1000);
          return;
        }

        if (regResult.status === 'FAILED') {
          logger.warn('Event previously failed. Acknowledging and skipping.', { event_id });
          channel.ack(msg);
          return;
        }
      }

      // 2. Dispatch Notification
      logger.info('Starting external notification dispatch simulation', { event_id });
      // Get current retry count from headers (defaults to 0 in Phase 4)
      const retryCount = (msg.properties.headers && msg.properties.headers['x-retry-count']) || 0;
      await mockDispatchNotification(event, retryCount);

      // 3. Mark as COMPLETED and log success
      await db.updateEventStatus(event_id, 'COMPLETED');
      await db.logNotification(event_id, recipient, type, payload, 'SENT');

      logger.info('Notification processed successfully', { event_id });
      channel.ack(msg);

    } catch (err) {
      logger.error('Error processing notification event', { event_id, error: err.message });

      try {
        await db.updateEventStatus(event_id, 'FAILED');
        await db.logNotification(event_id, recipient, type, payload, 'FAILED_EXTERNAL');
      } catch (dbErr) {
        logger.error('Failed to update DB state after processing failure', { event_id, error: dbErr.message });
      }

      channel.ack(msg);
    }
  });
}

module.exports = {
  startConsumer
};
