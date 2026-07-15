const { connectMQ, QUEUE_NAME, DLQ_NAME, publishToQueue } = require('../services/mq');
const db = require('../services/db');
const config = require('../config');
const { mockDispatchNotification } = require('../services/notification');
const logger = require('../utils/logger');

async function startConsumer() {
  const { channel } = await connectMQ();

  await channel.prefetch(10);

  logger.info(`Starting consumer for queue: ${QUEUE_NAME} with maxRetries: ${config.retry.maxRetries}`);

  channel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    let event;
    try {
      const contentStr = msg.content.toString();
      event = JSON.parse(contentStr);
    } catch (err) {
      logger.error('Received malformed message, routing directly to DLQ', { error: err.message });
      try {
        await publishToQueue(DLQ_NAME, msg.content, {
          headers: { 'x-original-error': 'Malformed JSON' }
        });
      } catch (dlqErr) {
        logger.error('Failed to route malformed message to DLQ', { error: dlqErr.message });
      }
      channel.ack(msg);
      return;
    }

    const { event_id, type, recipient, payload } = event;
    logger.info('Consumed message from queue', { event_id, type, recipient });

    // Retrieve retry count from headers
    const retryCount = (msg.properties.headers && msg.properties.headers['x-retry-count']) || 0;

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
          // Sleep for 1 second before requeueing to avoid connection thrashing
          setTimeout(() => {
            try {
              channel.nack(msg, false, true); // requeue = true
            } catch (err) {
              logger.error('Failed to requeue message', { event_id, error: err.message });
            }
          }, 1000);
          return;
        }

        if (regResult.status === 'FAILED') {
          logger.warn('Event previously failed permanently. Skipping.', { event_id });
          channel.ack(msg);
          return;
        }
      }

      // 2. Dispatch Notification
      logger.info('Attempting notification dispatch', { event_id, attempt: retryCount + 1 });
      await mockDispatchNotification(event, retryCount);

      // 3. Mark as COMPLETED and log success
      await db.updateEventStatus(event_id, 'COMPLETED');
      await db.logNotification(event_id, recipient, type, payload, 'SENT');

      logger.info('Notification processed successfully', { event_id });
      channel.ack(msg);

    } catch (err) {
      const isPermanent = err.isPermanent === true;
      logger.error('Error processing notification event', {
        event_id,
        error: err.message,
        isPermanent,
        retryCount
      });

      // Handle Failure
      if (isPermanent || retryCount >= config.retry.maxRetries) {
        const reason = isPermanent ? 'Permanent Failure' : `Max retries (${config.retry.maxRetries}) exhausted`;
        logger.warn(`Moving event to DLQ. Reason: ${reason}`, { event_id });

        try {
          // Update database state
          await db.updateEventStatus(event_id, 'FAILED');
          await db.logNotification(event_id, recipient, type, payload, 'DLQ_MOVED');

          // Publish to Dead-Letter Queue
          await publishToQueue(DLQ_NAME, event, {
            headers: {
              'x-retry-count': retryCount,
              'x-original-error': err.message,
              'x-failure-reason': reason
            }
          });
        } catch (failErr) {
          logger.error('Failed to complete DLQ routing operations', { event_id, error: failErr.message });
        }

        channel.ack(msg);
      } else {
        // Transient error -> execute backoff retry
        const nextAttempt = retryCount + 1;
        const delaySeconds = config.retry.initialDelay * Math.pow(config.retry.backoffFactor, nextAttempt - 1);
        const delayMs = delaySeconds * 1000;
        const delayQueueName = `delay_queue_${delaySeconds}s`;

        logger.info(`Routing event to delay queue for backoff retry`, {
          event_id,
          nextAttempt,
          delaySeconds,
          delayQueueName
        });

        try {
          // Update DB status to RETRYING so that subsequent retry attempts can be processed
          await db.updateEventStatus(event_id, 'RETRYING');

          // Declare delay queue dynamically with TTL and dead-letter routing to main queue
          await channel.assertQueue(delayQueueName, {
            durable: true,
            arguments: {
              'x-dead-letter-exchange': '',
              'x-dead-letter-routing-key': QUEUE_NAME,
              'x-message-ttl': delayMs
            }
          });

          // Publish back to RabbitMQ (into the delay queue)
          await publishToQueue(delayQueueName, event, {
            headers: {
              'x-retry-count': nextAttempt
            }
          });
          
          logger.info(`Event scheduled for retry`, { event_id, delaySeconds });
        } catch (retryErr) {
          logger.error('Failed to schedule event retry, requeueing to main queue instead', { event_id, error: retryErr.message });
          // Fallback: sleep and requeue
          setTimeout(() => {
            try {
              channel.nack(msg, false, true);
            } catch (nackErr) {
              logger.error('Failed to requeue message', { event_id, error: nackErr.message });
            }
          }, 1000);
          return;
        }

        channel.ack(msg);
      }
    }
  });
}

module.exports = {
  startConsumer
};
