const app = require('./app');
const config = require('./config');
const logger = require('./utils/logger');
const { connectMQ, closeMQ } = require('./services/mq');
const { getPool, closeDB } = require('./services/db');
const { startConsumer } = require('./consumer');

let server;

async function shutdown(signal) {
  logger.warn(`Received shutdown signal (${signal}). Initiating graceful shutdown...`);

  // Set a fallback force exit timeout of 10s
  const forceExitTimeout = setTimeout(() => {
    logger.error('Graceful shutdown timed out, force exiting');
    process.exit(1);
  }, 10000);

  try {
    if (server) {
      await new Promise((resolve) => {
        server.close(() => {
          logger.info('HTTP server closed');
          resolve();
        });
      });
    }

    // Close RabbitMQ connection (stops consumer, handles in-flight messages)
    await closeMQ();

    // Close MySQL connection pool
    await closeDB();

    clearTimeout(forceExitTimeout);
    logger.info('Graceful shutdown completed successfully');
    process.exit(0);
  } catch (err) {
    logger.error('Error occurred during graceful shutdown', { error: err.message });
    process.exit(1);
  }
}

async function bootstrap() {
  try {
    logger.info('Starting service bootstrap...');

    // Initialize Database Connection Pool
    getPool();
    logger.info('Database connection pool initialized');

    // Initialize RabbitMQ connection and channels
    await connectMQ();

    // Start processing messages
    await startConsumer();

    // Start API HTTP Server
    server = app.listen(config.port, () => {
      logger.info(`Notification service API listening on port ${config.port} in ${config.nodeEnv} mode`);
    });

    // Signal listeners for graceful shutdown
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));

  } catch (err) {
    logger.error('Bootstrap failed, shutting down service', { error: err.message });
    process.exit(1);
  }
}

bootstrap();
