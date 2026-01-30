const KafkaProducer = require('../src/kafka/producer');

async function main() {
  // Option 1: Use environment variables (from .env file)
  const producer = new KafkaProducer();

  try {
    // Connect to Kafka
    await producer.connect();

    // Send a simple message
    await producer.send('test-topic', {
      value: 'Hello Kafka!',
    });

    // Send a message with key
    await producer.send('test-topic', {
      key: 'user-123',
      value: JSON.stringify({ name: 'John Doe', action: 'login' }),
    });

    // Send multiple messages
    await producer.send('test-topic', [
      { key: 'msg-1', value: 'First message' },
      { key: 'msg-2', value: 'Second message' },
      { key: 'msg-3', value: JSON.stringify({ id: 3, text: 'Third message' }) },
    ]);

    console.log('All messages sent successfully!');
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await producer.disconnect();
  }
}

main();
