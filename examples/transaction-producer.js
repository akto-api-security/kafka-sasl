const KafkaProducer = require('../src/kafka/producer');

async function main() {
  const producer = new KafkaProducer({
    transactionalId: 'my-transactional-producer',
    idempotent: true,
  });

  try {
    await producer.connect();

    // Use transactions for atomic operations
    await producer.transaction(async (transaction) => {
      await transaction.send({
        topic: 'accounts',
        messages: [
          {
            key: 'account-1',
            value: JSON.stringify({ account: 'A', amount: -100 }),
          },
        ],
      });

      await transaction.send({
        topic: 'accounts',
        messages: [
          {
            key: 'account-2',
            value: JSON.stringify({ account: 'B', amount: +100 }),
          },
        ],
      });
    });

    console.log('Transaction completed successfully!');
  } catch (error) {
    console.error('Transaction failed:', error);
  } finally {
    await producer.disconnect();
  }
}

main();
