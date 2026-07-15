const request = require('supertest');
const app = require('../../src/app');
const db = require('../../src/services/db');
const { connectMQ, DLQ_NAME } = require('../../src/services/mq');

describe('Notification Service End-to-End Integration Tests', () => {
  let mqChannel;

  beforeAll(async () => {
    db.getPool();
    const { channel } = await connectMQ();
    mqChannel = channel;
  });

  afterAll(async () => {
    await db.closeDB();
    const { closeMQ } = require('../../src/services/mq');
    await closeMQ();
  });

  beforeEach(async () => {
    const pool = db.getPool();
    await pool.query('SET FOREIGN_KEY_CHECKS = 0');
    await pool.query('TRUNCATE TABLE notification_logs');
    await pool.query('TRUNCATE TABLE processed_events');
    await pool.query('SET FOREIGN_KEY_CHECKS = 1');

    try {
      await mqChannel.purgeQueue(DLQ_NAME);
    } catch (err) {
      // ignore queue purge error if any
    }
  });

  // Polling helper to wait for the event status in the database dynamically
  async function waitForEventStatus(eventId, expectedStatus, timeoutMs = 12000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const status = await db.getEventStatus(eventId);
      if (status === expectedStatus) {
        return status;
      }
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    const finalStatus = await db.getEventStatus(eventId);
    throw new Error(`Timeout waiting for event ${eventId} to become ${expectedStatus}. Current status: ${finalStatus}`);
  }

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  it('should successfully publish, consume, mock dispatch and log a notification', async () => {
    const event = {
      event_id: '11cf4e81-cf19-4b6a-93ef-62e92c608f65',
      type: 'email',
      recipient: 'integration-test@example.com',
      payload: { subject: 'Integration Hello', body: 'This is an E2E integration test' },
      timestamp: new Date().toISOString()
    };

    // 1. Publish Event via API
    const response = await request(app)
      .post('/api/v1/publish-notification-event')
      .send(event);

    expect(response.status).toBe(202);
    expect(response.body.event_id).toBe(event.event_id);

    // 2. Wait for status to become COMPLETED
    await waitForEventStatus(event.event_id, 'COMPLETED', 4000);

    // 3. Assert Notification Logs
    const pool = db.getPool();
    const [logs] = await pool.query('SELECT * FROM notification_logs WHERE event_id = ?', [event.event_id]);
    expect(logs.length).toBe(1);
    expect(logs[0].status).toBe('SENT');
    expect(logs[0].recipient).toBe(event.recipient);
  });

  it('should enforce idempotency and skip processing duplicate event_ids', async () => {
    const event = {
      event_id: '22cf4e81-cf19-4b6a-93ef-62e92c608f65',
      type: 'sms',
      recipient: '+12345678902',
      payload: { body: 'SMS E2E message' },
      timestamp: new Date().toISOString()
    };

    // Publish event first time
    const res1 = await request(app).post('/api/v1/publish-notification-event').send(event);
    expect(res1.status).toBe(202);

    await waitForEventStatus(event.event_id, 'COMPLETED', 4000);

    // Publish duplicate event second time
    const res2 = await request(app).post('/api/v1/publish-notification-event').send(event);
    expect(res2.status).toBe(202);

    // Sleep for 1 second to ensure that if a second consumption occurred, it would have run
    await sleep(1000);

    // Verify only one log entry is written for the event_id (meaning consumer skipped reprocessing)
    const pool = db.getPool();
    const [logs] = await pool.query('SELECT * FROM notification_logs WHERE event_id = ?', [event.event_id]);
    expect(logs.length).toBe(1);
  });

  it('should retry transient failures and eventually succeed', async () => {
    const event = {
      event_id: '33cf4e81-cf19-4b6a-93ef-62e92c608f65',
      type: 'push',
      recipient: 'device_user_33',
      payload: { title: 'push', fail_attempts: 1 }, // Fails 1st attempt, succeeds on 2nd
      timestamp: new Date().toISOString()
    };

    // Publish
    const response = await request(app).post('/api/v1/publish-notification-event').send(event);
    expect(response.status).toBe(202);

    // Wait for the retry to resolve and complete
    await waitForEventStatus(event.event_id, 'COMPLETED', 5000);

    const pool = db.getPool();
    const [logs] = await pool.query('SELECT * FROM notification_logs WHERE event_id = ?', [event.event_id]);
    expect(logs.length).toBe(1);
    expect(logs[0].status).toBe('SENT');
  }, 10000);

  it('should route event to DLQ after exhausting retry attempts', async () => {
    const event = {
      event_id: '44cf4e81-cf19-4b6a-93ef-62e92c608f65',
      type: 'email',
      recipient: 'dlq-test@example.com',
      // config.retry.maxRetries is 3. We fail 4 times to exhaust maxRetries.
      // Delays: retry 1 (1s), retry 2 (2s), retry 3 (4s). Total delays = 7s.
      payload: { subject: 'Fail exhaustion', fail_attempts: 4 },
      timestamp: new Date().toISOString()
    };

    // Publish
    const response = await request(app).post('/api/v1/publish-notification-event').send(event);
    expect(response.status).toBe(202);

    // Wait for retry exhaustion to mark as FAILED (using 12s timeout to cover 7s delays)
    await waitForEventStatus(event.event_id, 'FAILED', 12000);

    // Check Notification Logs contains DLQ_MOVED
    const pool = db.getPool();
    const [logs] = await pool.query('SELECT * FROM notification_logs WHERE event_id = ?', [event.event_id]);
    expect(logs.length).toBe(1);
    expect(logs[0].status).toBe('DLQ_MOVED');

    // Fetch the message from DLQ in RabbitMQ and assert its properties
    const msg = await mqChannel.get(DLQ_NAME, { noAck: false });
    expect(msg).not.toBeNull();
    expect(msg).not.toBe(false);

    const dlqEvent = JSON.parse(msg.content.toString());
    expect(dlqEvent.event_id).toBe(event.event_id);
    expect(msg.properties.headers['x-failure-reason']).toContain('Max retries');
  }, 15000);

  it('should immediately route permanent failures to DLQ without retry', async () => {
    const event = {
      event_id: '55cf4e81-cf19-4b6a-93ef-62e92c608f65',
      type: 'push',
      recipient: 'device_user_55',
      payload: { title: 'Permanent Fail', should_fail: true },
      timestamp: new Date().toISOString()
    };

    // Publish
    const response = await request(app).post('/api/v1/publish-notification-event').send(event);
    expect(response.status).toBe(202);

    // Wait for status to become FAILED
    await waitForEventStatus(event.event_id, 'FAILED', 4000);

    // Check Logs updated to DLQ_MOVED
    const pool = db.getPool();
    const [logs] = await pool.query('SELECT * FROM notification_logs WHERE event_id = ?', [event.event_id]);
    expect(logs.length).toBe(1);
    expect(logs[0].status).toBe('DLQ_MOVED');

    // Fetch DLQ message
    const msg = await mqChannel.get(DLQ_NAME, { noAck: false });
    expect(msg).not.toBeNull();
    expect(msg).not.toBe(false);

    const dlqEvent = JSON.parse(msg.content.toString());
    expect(dlqEvent.event_id).toBe(event.event_id);
    expect(msg.properties.headers['x-failure-reason']).toContain('Permanent Failure');
  });
});
