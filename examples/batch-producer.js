const KafkaProducer = require('../src/kafka/producer');

async function main() {
  const producer = new KafkaProducer();

  try {
    await producer.connect();

    // Send batch messages to multiple topics
    await producer.sendBatch([
      {
        topic: 'topic-1',
        messages: [
          { key: 'key1', value: 'message 1 for topic 1' },
          { key: 'key2', value: 'message 2 for topic 1' },
        ],
      },
      {
        topic: 'topic-2',
        messages: [
          { key: 'key3', value: 'message 1 for topic 2' },
          { key: 'key4', value: 'message 2 for topic 2' },
        ],
      },
    ]);

    console.log('Batch messages sent successfully!');
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await producer.disconnect();
  }
}

main();
