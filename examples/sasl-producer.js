const KafkaProducer = require('../src/kafka/producer');

async function main() {
  // Option 2: Use custom configuration with SASL authentication
  const producer = new KafkaProducer({
    brokers: ['your-kafka-broker:9093'],
    clientId: 'my-app-producer',
    saslEnabled: true,
    saslMechanism: 'plain', // or 'scram-sha-256', 'scram-sha-512', 'aws'
    saslUsername: 'your-username',
    saslPassword: 'your-password',
    sslEnabled: true,
    defaultTopic: 'my-default-topic',
  });

  try {
    await producer.connect();

    // Send to default topic
    await producer.send(null, {
      value: JSON.stringify({
        timestamp: new Date().toISOString(),
        message: 'Hello from SASL authenticated producer',
      }),
    });

    // Send to specific topic
    await producer.send('events-topic', {
      key: 'event-001',
      value: JSON.stringify({
        eventType: 'user.created',
        userId: '12345',
        timestamp: Date.now(),
      }),
      headers: {
        'correlation-id': Buffer.from('abc-123'),
        'source': Buffer.from('user-service'),
      },
    });

    console.log('Messages sent with SASL authentication!');
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await producer.disconnect();
  }
}

main();
