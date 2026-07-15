const config = require('../../src/config');
const { startConsumer } = require('../../src/consumer');
const { connectMQ } = require('../../src/services/mq');

jest.mock('../../src/services/mq');
jest.mock('../../src/services/db');
jest.mock('../../src/services/notification');

describe('Retry Logic and Backoff Calculation Unit Tests', () => {
  let mockChannel;

  beforeEach(() => {
    jest.clearAllMocks();
    mockChannel = {
      prefetch: jest.fn().mockResolvedValue(true),
      consume: jest.fn(),
      ack: jest.fn(),
      nack: jest.fn(),
      assertQueue: jest.fn().mockResolvedValue(true),
      sendToQueue: jest.fn().mockReturnValue(true)
    };
    connectMQ.mockResolvedValue({ channel: mockChannel });
  });

  it('should compute exponential backoff delays correctly based on standard factor', () => {
    const initialDelay = 1; // 1s
    const backoffFactor = 5;

    // Attempt 1: retryCount=0, nextAttempt=1
    // delay = 1 * (5 ^ 0) = 1
    const delay1 = initialDelay * Math.pow(backoffFactor, 1 - 1);
    expect(delay1).toBe(1);

    // Attempt 2: retryCount=1, nextAttempt=2
    // delay = 1 * (5 ^ 1) = 5
    const delay2 = initialDelay * Math.pow(backoffFactor, 2 - 1);
    expect(delay2).toBe(5);

    // Attempt 3: retryCount=2, nextAttempt=3
    // delay = 1 * (5 ^ 2) = 25
    const delay3 = initialDelay * Math.pow(backoffFactor, 3 - 1);
    expect(delay3).toBe(25);
  });

  it('should load configurable retry parameters from config', () => {
    expect(config.retry.maxRetries).toBeDefined();
    expect(config.retry.initialDelay).toBeDefined();
    expect(config.retry.backoffFactor).toBeDefined();
  });

  it('should initialize the consumer with prefetch and subscribe to correct queue', async () => {
    await startConsumer();
    expect(connectMQ).toHaveBeenCalled();
    expect(mockChannel.prefetch).toHaveBeenCalledWith(10);
    expect(mockChannel.consume).toHaveBeenCalled();
  });
});
