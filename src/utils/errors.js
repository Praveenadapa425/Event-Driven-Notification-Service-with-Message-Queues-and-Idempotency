class PermanentError extends Error {
  constructor(message) {
    super(message);
    this.name = 'PermanentError';
    this.isPermanent = true;
  }
}

class TransientError extends Error {
  constructor(message) {
    super(message);
    this.name = 'TransientError';
    this.isTransient = true;
  }
}

module.exports = {
  PermanentError,
  TransientError
};
