const { validateNotificationEvent } = require('../../src/utils/validation');

describe('NotificationEvent Validation Unit Tests', () => {
  const validEmailEvent = {
    event_id: 'a9cf4e81-cf19-4b6a-93ef-62e92c608f65',
    type: 'email',
    recipient: 'test@example.com',
    payload: { subject: 'Hello', body: 'World' },
    timestamp: '2026-07-15T12:00:00.000Z'
  };

  const validSmsEvent = {
    event_id: 'b8cf4e81-cf19-4b6a-93ef-62e92c608f65',
    type: 'sms',
    recipient: '+12345678901',
    payload: { body: 'SMS message' },
    timestamp: '2026-07-15T12:00:00.000Z'
  };

  const validPushEvent = {
    event_id: 'c7cf4e81-cf19-4b6a-93ef-62e92c608f65',
    type: 'push',
    recipient: 'user_device_token_abc123',
    payload: { title: 'New alert', body: 'Push notification details' },
    timestamp: '2026-07-15T12:00:00.000Z'
  };

  it('should validate a correct email event successfully', () => {
    const { error, value } = validateNotificationEvent(validEmailEvent);
    expect(error).toBeUndefined();
    expect(value).toEqual(validEmailEvent);
  });

  it('should validate a correct sms event successfully', () => {
    const { error, value } = validateNotificationEvent(validSmsEvent);
    expect(error).toBeUndefined();
    expect(value).toEqual(validSmsEvent);
  });

  it('should validate a correct push event successfully', () => {
    const { error, value } = validateNotificationEvent(validPushEvent);
    expect(error).toBeUndefined();
    expect(value).toEqual(validPushEvent);
  });

  it('should reject invalid UUIDs for event_id', () => {
    const invalidEvent = { ...validEmailEvent, event_id: 'not-a-uuid' };
    const { error } = validateNotificationEvent(invalidEvent);
    expect(error).toBeDefined();
    expect(error.details[0].message).toContain('event_id');
  });

  it('should reject invalid types', () => {
    const invalidEvent = { ...validEmailEvent, type: 'slack' };
    const { error } = validateNotificationEvent(invalidEvent);
    expect(error).toBeDefined();
    expect(error.details[0].message).toContain('type');
  });

  it('should reject invalid email for email type', () => {
    const invalidEvent = { ...validEmailEvent, recipient: 'not-an-email' };
    const { error } = validateNotificationEvent(invalidEvent);
    expect(error).toBeDefined();
    expect(error.details[0].message).toContain('recipient');
  });

  it('should reject invalid phone for sms type', () => {
    const invalidEvent = { ...validSmsEvent, recipient: 'not-a-phone-number' };
    const { error } = validateNotificationEvent(invalidEvent);
    expect(error).toBeDefined();
    expect(error.details[0].message).toContain('recipient');
  });

  it('should reject missing fields', () => {
    const invalidEvent = { ...validEmailEvent };
    delete invalidEvent.timestamp;
    const { error } = validateNotificationEvent(invalidEvent);
    expect(error).toBeDefined();
    expect(error.details[0].message).toContain('timestamp');
  });
});
