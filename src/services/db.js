const mysql = require('mysql2/promise');
const config = require('../config');
const logger = require('../utils/logger');

let pool = null;

function getPool() {
  if (!pool) {
    logger.info(`Creating MySQL pool for host: ${config.db.host}`);
    pool = mysql.createPool({
      host: config.db.host,
      port: config.db.port,
      user: config.db.user,
      password: config.db.password,
      database: config.db.database,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0
    });
  }
  return pool;
}

async function getEventStatus(eventId) {
  const currentPool = getPool();
  const [rows] = await currentPool.query('SELECT status FROM processed_events WHERE event_id = ?', [eventId]);
  if (rows.length === 0) return null;
  return rows[0].status;
}

/**
 * Atomically attempts to register a new event as PROCESSING.
 * If the event already exists with status 'RETRYING', it transitions it to 'PROCESSING'.
 * If the event is COMPLETED, PROCESSING, or FAILED, it returns the current status.
 */
async function tryRegisterEvent(eventId) {
  const currentPool = getPool();
  try {
    // 1. Attempt to insert as PROCESSING (first-time delivery)
    await currentPool.query('INSERT INTO processed_events (event_id, status) VALUES (?, ?)', [eventId, 'PROCESSING']);
    return { success: true, status: 'PROCESSING' };
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      // 2. Attempt to transition status from RETRYING back to PROCESSING (retry delivery)
      const [updateResult] = await currentPool.query(
        "UPDATE processed_events SET status = 'PROCESSING' WHERE event_id = ? AND status = 'RETRYING'",
        [eventId]
      );
      if (updateResult.affectedRows === 1) {
        logger.info('Transitioned event from RETRYING to PROCESSING', { eventId });
        return { success: true, status: 'PROCESSING' };
      }

      // 3. If no row affected, it is actively processing, completed, or failed. Fetch current status.
      const currentStatus = await getEventStatus(eventId);
      return { success: false, status: currentStatus };
    }
    logger.error('Error during atomic event registration', { eventId, error: err.message });
    throw err;
  }
}

async function updateEventStatus(eventId, status) {
  const currentPool = getPool();
  try {
    await currentPool.query('UPDATE processed_events SET status = ? WHERE event_id = ?', [status, eventId]);
    logger.info('Updated event status', { eventId, status });
  } catch (err) {
    logger.error('Failed to update event status', { eventId, status, error: err.message });
    throw err;
  }
}

async function logNotification(eventId, recipient, type, payload, status) {
  const currentPool = getPool();
  try {
    const payloadStr = typeof payload === 'string' ? payload : JSON.stringify(payload);
    const [result] = await currentPool.query(
      'INSERT INTO notification_logs (event_id, recipient, type, message_payload, status) VALUES (?, ?, ?, ?, ?)',
      [eventId, recipient, type, payloadStr, status]
    );
    logger.info('Inserted notification log', { eventId, recipient, type, status, logId: result.insertId });
    return result.insertId;
  } catch (err) {
    logger.error('Failed to insert notification log', { eventId, recipient, type, status, error: err.message });
    throw err;
  }
}

async function closeDB() {
  if (pool) {
    await pool.end();
    logger.info('MySQL connection pool closed gracefully');
    pool = null;
  }
}

module.exports = {
  getPool,
  getEventStatus,
  tryRegisterEvent,
  updateEventStatus,
  logNotification,
  closeDB
};
