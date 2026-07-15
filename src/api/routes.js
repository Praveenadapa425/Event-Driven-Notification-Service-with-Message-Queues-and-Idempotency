const express = require('express');
const router = express.Router();
const { validateNotificationEvent } = require('../utils/validation');
const { publishToQueue, QUEUE_NAME } = require('../services/mq');
const logger = require('../utils/logger');

router.post('/publish-notification-event', async (req, res) => {
  const { error, value } = validateNotificationEvent(req.body);
  if (error) {
    logger.warn('Publish request payload validation failed', {
      errors: error.details.map(d => d.message),
      payload: req.body
    });
    return res.status(400).json({
      error: 'Bad Request',
      details: error.details.map(d => d.message)
    });
  }

  try {
    await publishToQueue(QUEUE_NAME, value, {
      headers: {
        'x-retry-count': 0
      }
    });

    logger.info('Notification event successfully published to queue', { event_id: value.event_id });

    return res.status(202).json({
      message: 'Accepted',
      event_id: value.event_id
    });
  } catch (err) {
    logger.error('Error publishing event to MQ', {
      event_id: req.body ? req.body.event_id : undefined,
      error: err.message
    });
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to publish event to message queue'
    });
  }
});

module.exports = router;
