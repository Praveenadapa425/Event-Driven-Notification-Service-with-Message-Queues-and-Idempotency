const app = require('./app');
const config = require('./config');
const logger = require('./utils/logger');

const server = app.listen(config.port, () => {
  logger.info(`Notification service listening on port ${config.port} in ${config.nodeEnv} mode`);
});
