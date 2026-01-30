#!/usr/bin/env node
require('dotenv').config();
const { Kafka } = require('kafkajs');

async function publishMessage() {
  const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || 'simple-producer',
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    sasl: process.env.KAFKA_SASL_ENABLED === 'true' ? {
      mechanism: process.env.KAFKA_SASL_MECHANISM || 'plain',
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD,
    } : null,
  });

  const producer = kafka.producer();

  try {
    console.log('Connecting to Kafka...');
    await producer.connect();
    console.log('✓ Connected');

    const topic = process.env.KAFKA_TOPIC || 'test-topic';
    const message = process.argv[2] || 'Hello from simple script!';

    console.log(`Publishing to topic: ${topic}`);
    await producer.send({
      topic: topic,
      messages: [{ value: message, timestamp: Date.now().toString() }],
    });

    console.log('✓ Message published successfully!');
    console.log(`Message: ${message}`);
  } catch (error) {
    console.error('✗ Error:', error.message);
    process.exit(1);
  } finally {
    await producer.disconnect();
    console.log('Disconnected');
  }
}

publishMessage();
