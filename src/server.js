const app = require('./app');
const config = require('./config');
const logger = require('./utils/logger');
const { connectMQ } = require('./services/mq');
const { getPool } = require('./services/db');
const { startConsumer } = require('./consumer');

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
    const server = app.listen(config.port, () => {
      logger.info(`Notification service API listening on port ${config.port} in ${config.nodeEnv} mode`);
    });

  } catch (err) {
    logger.error('Bootstrap failed, shutting down service', { error: err.message });
    process.exit(1);
  }
}

bootstrap();
