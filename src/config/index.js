require('dotenv').config();

module.exports = {
  port: parseInt(process.env.PORT, 10) || 8080,
  nodeEnv: process.env.NODE_ENV || 'development',
  
  db: {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT, 10) || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASS || 'rootpassword',
    database: process.env.DB_NAME || 'notifications_db'
  },
  
  mq: {
    host: process.env.MQ_HOST || 'localhost',
    port: parseInt(process.env.MQ_PORT, 10) || 5672,
    user: process.env.MQ_USER || 'guest',
    password: process.env.MQ_PASS || 'guest',
    get url() {
      return `amqp://${this.user}:${this.password}@${this.host}:${this.port}`;
    }
  },
  
  retry: {
    maxRetries: parseInt(process.env.MAX_RETRIES, 10) || 3,
    initialDelay: parseInt(process.env.RETRY_INITIAL_DELAY, 10) || 1, // in seconds
    backoffFactor: parseInt(process.env.RETRY_BACKOFF_FACTOR, 10) || 5
  }
};
