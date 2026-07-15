const db = require('../../src/services/db');

describe('Idempotency and DB Operations Unit Tests', () => {
  beforeAll(async () => {
    db.getPool();
  });

  afterAll(async () => {
    await db.closeDB();
  });

  beforeEach(async () => {
    const pool = db.getPool();
    await pool.query('SET FOREIGN_KEY_CHECKS = 0');
    await pool.query('TRUNCATE TABLE notification_logs');
    await pool.query('TRUNCATE TABLE processed_events');
    await pool.query('SET FOREIGN_KEY_CHECKS = 1');
  });

  it('should successfully register a new event as PROCESSING', async () => {
    const eventId = 'd6cf4e81-cf19-4b6a-93ef-62e92c608f65';
    const result = await db.tryRegisterEvent(eventId);
    expect(result.success).toBe(true);
    expect(result.status).toBe('PROCESSING');

    const status = await db.getEventStatus(eventId);
    expect(status).toBe('PROCESSING');
  });

  it('should reject a duplicate event registration and return current status', async () => {
    const eventId = 'e5cf4e81-cf19-4b6a-93ef-62e92c608f65';

    // First registration
    const result1 = await db.tryRegisterEvent(eventId);
    expect(result1.success).toBe(true);

    // Second registration (should fail)
    const result2 = await db.tryRegisterEvent(eventId);
    expect(result2.success).toBe(false);
    expect(result2.status).toBe('PROCESSING');

    // Update to COMPLETED
    await db.updateEventStatus(eventId, 'COMPLETED');

    // Third registration (should fail, returning COMPLETED)
    const result3 = await db.tryRegisterEvent(eventId);
    expect(result3.success).toBe(false);
    expect(result3.status).toBe('COMPLETED');
  });

  it('should successfully update event status', async () => {
    const eventId = 'f4cf4e81-cf19-4b6a-93ef-62e92c608f65';
    await db.tryRegisterEvent(eventId);

    await db.updateEventStatus(eventId, 'FAILED');
    const status = await db.getEventStatus(eventId);
    expect(status).toBe('FAILED');
  });

  it('should successfully log notifications with foreign keys', async () => {
    const eventId = '03cf4e81-cf19-4b6a-93ef-62e92c608f65';
    await db.tryRegisterEvent(eventId);
    await db.updateEventStatus(eventId, 'COMPLETED');

    const logId = await db.logNotification(
      eventId,
      'test@example.com',
      'email',
      { subject: 'Hello' },
      'SENT'
    );
    expect(logId).toBeGreaterThan(0);
  });
});
