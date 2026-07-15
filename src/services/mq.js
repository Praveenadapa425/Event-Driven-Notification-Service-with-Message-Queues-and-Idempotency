const amqp = require('amqplib');
const config = require('../config');
const logger = require('../utils/logger');

let connection = null;
let channel = null;

const QUEUE_NAME = 'notification_events';
const DLQ_NAME = 'notification_dead_letter_queue';

async function connectMQ() {
  if (connection && channel) {
    return { connection, channel };
  }

  try {
    logger.info(`Connecting to RabbitMQ at ${config.mq.url.replace(/:[^:]*@/, ':****@')}`);
    connection = await amqp.connect(config.mq.url);
    channel = await connection.createChannel();

    // Declare the dead-letter queue durably
    await channel.assertQueue(DLQ_NAME, {
      durable: true
    });

    // Declare the main queue durably
    await channel.assertQueue(QUEUE_NAME, {
      durable: true
    });

    logger.info('Connected to RabbitMQ and declared queues');

    connection.on('error', (err) => {
      logger.error('RabbitMQ connection error', { error: err.message });
    });

    connection.on('close', () => {
      logger.warn('RabbitMQ connection closed');
      connection = null;
      channel = null;
    });

    return { connection, channel };
  } catch (err) {
    logger.error('Failed to connect to RabbitMQ', { error: err.message });
    throw err;
  }
}

async function publishToQueue(queue, message, options = {}) {
  if (!channel) {
    await connectMQ();
  }
  try {
    const payloadStr = typeof message === 'string' ? message : JSON.stringify(message);
    const published = channel.sendToQueue(queue, Buffer.from(payloadStr), {
      persistent: true,
      ...options
    });
    if (!published) {
      throw new Error(`Queue write buffer full for ${queue}`);
    }
    return published;
  } catch (err) {
    logger.error(`Failed to publish message to queue ${queue}`, { error: err.message });
    throw err;
  }
}

async function closeMQ() {
  try {
    if (channel) {
      await channel.close();
    }
    if (connection) {
      await connection.close();
    }
    logger.info('RabbitMQ connection closed gracefully');
  } catch (err) {
    logger.error('Error closing RabbitMQ connection', { error: err.message });
  } finally {
    connection = null;
    channel = null;
  }
}

module.exports = {
  connectMQ,
  publishToQueue,
  closeMQ,
  getChannel: () => channel,
  getConnection: () => connection,
  QUEUE_NAME,
  DLQ_NAME
};
