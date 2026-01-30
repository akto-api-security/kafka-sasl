require('dotenv').config();
const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const KafkaProducer = require('./kafka/producer');

const app = express();
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(morgan('combined'));

// Initialize Kafka Producer
let producer;
let isProducerReady = false;

async function initializeProducer() {
  try {
    producer = new KafkaProducer();
    await producer.connect();
    isProducerReady = true;
    console.log('✓ Kafka producer initialized and connected');
  } catch (error) {
    console.error('✗ Failed to initialize Kafka producer:', error.message);
    isProducerReady = false;
    // Retry connection after 5 seconds
    setTimeout(initializeProducer, 5000);
  }
}

// Middleware to check producer status
function checkProducerReady(req, res, next) {
  if (!isProducerReady) {
    return res.status(503).json({
      success: false,
      error: 'Kafka producer is not ready. Please try again later.',
    });
  }
  next();
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    success: true,
    status: 'healthy',
    kafka: {
      connected: isProducerReady,
      brokers: process.env.KAFKA_BROKERS,
      saslEnabled: process.env.KAFKA_SASL_ENABLED === 'true',
    },
    timestamp: new Date().toISOString(),
  });
});

// Publish a single message
app.post('/api/publish', checkProducerReady, async (req, res) => {
  try {
    const { topic, message, key, headers, partition } = req.body;

    if (!topic && !process.env.KAFKA_DEFAULT_TOPIC) {
      return res.status(400).json({
        success: false,
        error: 'Topic is required when no default topic is configured',
      });
    }

    if (!message) {
      return res.status(400).json({
        success: false,
        error: 'Message is required',
      });
    }

    const kafkaMessage = {
      value: typeof message === 'string' ? message : JSON.stringify(message),
      key: key || null,
      headers: headers || {},
      partition: partition,
    };

    const result = await producer.send(topic || null, kafkaMessage);

    res.json({
      success: true,
      message: 'Message published successfully',
      topic: topic || process.env.KAFKA_DEFAULT_TOPIC,
      result: result,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error('Error publishing message:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Publish multiple messages to a single topic
app.post('/api/publish/bulk', checkProducerReady, async (req, res) => {
  try {
    const { topic, messages } = req.body;

    if (!topic && !process.env.KAFKA_DEFAULT_TOPIC) {
      return res.status(400).json({
        success: false,
        error: 'Topic is required when no default topic is configured',
      });
    }

    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Messages array is required and must not be empty',
      });
    }

    const formattedMessages = messages.map(msg => {
      if (typeof msg === 'string' || typeof msg === 'object') {
        return {
          value: typeof msg === 'string' ? msg : JSON.stringify(msg),
          key: msg.key || null,
          headers: msg.headers || {},
          partition: msg.partition,
        };
      }
      return { value: String(msg) };
    });

    const result = await producer.send(topic || null, formattedMessages);

    res.json({
      success: true,
      message: `${formattedMessages.length} messages published successfully`,
      topic: topic || process.env.KAFKA_DEFAULT_TOPIC,
      count: formattedMessages.length,
      result: result,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error('Error publishing bulk messages:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Publish messages to multiple topics (batch)
app.post('/api/publish/batch', checkProducerReady, async (req, res) => {
  try {
    const { topicMessages } = req.body;

    if (!topicMessages || !Array.isArray(topicMessages) || topicMessages.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'topicMessages array is required',
        example: {
          topicMessages: [
            {
              topic: 'topic-1',
              messages: [
                { key: 'key1', value: 'message1' },
                { value: 'message2' },
              ],
            },
          ],
        },
      });
    }

    const result = await producer.sendBatch(topicMessages);

    res.json({
      success: true,
      message: 'Batch messages published successfully',
      topicCount: topicMessages.length,
      result: result,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error('Error publishing batch messages:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Get Kafka metadata
app.get('/api/metadata', checkProducerReady, async (req, res) => {
  try {
    const topics = req.query.topics ? req.query.topics.split(',') : [];
    const metadata = await producer.getMetadata(topics);

    res.json({
      success: true,
      metadata: metadata,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error('Error fetching metadata:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// API documentation
app.get('/api/docs', (req, res) => {
  res.json({
    endpoints: [
      {
        method: 'GET',
        path: '/health',
        description: 'Check API and Kafka connection health',
      },
      {
        method: 'POST',
        path: '/api/publish',
        description: 'Publish a single message to a topic',
        body: {
          topic: 'string (optional if default topic is set)',
          message: 'string or object (required)',
          key: 'string (optional)',
          headers: 'object (optional)',
          partition: 'number (optional)',
        },
      },
      {
        method: 'POST',
        path: '/api/publish/bulk',
        description: 'Publish multiple messages to a single topic',
        body: {
          topic: 'string (optional if default topic is set)',
          messages: 'array of messages (required)',
        },
      },
      {
        method: 'POST',
        path: '/api/publish/batch',
        description: 'Publish messages to multiple topics',
        body: {
          topicMessages: [
            {
              topic: 'string',
              messages: 'array of messages',
            },
          ],
        },
      },
      {
        method: 'GET',
        path: '/api/metadata',
        description: 'Get Kafka cluster metadata',
        query: {
          topics: 'comma-separated topic names (optional)',
        },
      },
    ],
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    name: 'Kafka Producer API',
    version: '1.0.0',
    documentation: '/api/docs',
    health: '/health',
  });
});

// Error handler
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({
    success: false,
    error: 'Internal server error',
    message: err.message,
  });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully...');
  if (producer && isProducerReady) {
    await producer.disconnect();
  }
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('\nSIGINT received, shutting down gracefully...');
  if (producer && isProducerReady) {
    await producer.disconnect();
  }
  process.exit(0);
});

// Start server
app.listen(PORT, HOST, () => {
  console.log(`\n🚀 Kafka Producer API Server started`);
  console.log(`   URL: http://${HOST}:${PORT}`);
  console.log(`   Health: http://${HOST}:${PORT}/health`);
  console.log(`   Docs: http://${HOST}:${PORT}/api/docs`);
  console.log(`\nInitializing Kafka producer...`);
  initializeProducer();
});
