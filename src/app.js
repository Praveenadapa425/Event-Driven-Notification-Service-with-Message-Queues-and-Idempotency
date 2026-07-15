const express = require('express');
const app = express();
const routes = require('./api/routes');

app.use(express.json());

// Mount API routes
app.use('/api/v1', routes);

// Health Check
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK' });
});

module.exports = app;
