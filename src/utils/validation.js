const Joi = require('joi');

const notificationEventSchema = Joi.object({
  event_id: Joi.string().uuid().required(),
  type: Joi.string().valid('email', 'sms', 'push').required(),
  recipient: Joi.string().required().when('type', {
    is: 'email',
    then: Joi.string().email(),
    otherwise: Joi.string().when('type', {
      is: 'sms',
      then: Joi.string().pattern(/^\+?[1-9]\d{1,14}$/, 'E.164 phone number'),
      otherwise: Joi.string().min(1)
    })
  }),
  payload: Joi.object().required().min(1),
  timestamp: Joi.string().isoDate().required()
});

function validateNotificationEvent(data) {
  return notificationEventSchema.validate(data, { abortEarly: false });
}

module.exports = {
  validateNotificationEvent
};
