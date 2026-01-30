const { Kafka, logLevel } = require('kafkajs');
const KafkaConfig = require('../config/kafka.config');

class KafkaProducer {
  constructor(customConfig = {}) {
    this.kafkaConfig = new KafkaConfig(customConfig);
    this.kafka = new Kafka(this.kafkaConfig.getConfig());
    this.producer = this.kafka.producer({
      idempotent: customConfig.idempotent !== undefined ? customConfig.idempotent : true,
      maxInFlightRequests: customConfig.maxInFlightRequests || 5,
      transactionalId: customConfig.transactionalId || undefined,
    });
    this.isConnected = false;
    this.defaultTopic = customConfig.defaultTopic || process.env.KAFKA_DEFAULT_TOPIC;
  }

  async connect() {
    try {
      await this.producer.connect();
      this.isConnected = true;
      console.log('Kafka producer connected successfully');
      return true;
    } catch (error) {
      console.error('Failed to connect Kafka producer:', error);
      throw error;
    }
  }

  async disconnect() {
    try {
      await this.producer.disconnect();
      this.isConnected = false;
      console.log('Kafka producer disconnected successfully');
      return true;
    } catch (error) {
      console.error('Failed to disconnect Kafka producer:', error);
      throw error;
    }
  }

  async send(topic, messages, options = {}) {
    if (!this.isConnected) {
      throw new Error('Producer is not connected. Call connect() first.');
    }

    try {
      const targetTopic = topic || this.defaultTopic;

      if (!targetTopic) {
        throw new Error('Topic is required. Provide a topic or set a default topic.');
      }

      // Normalize messages to array format
      const normalizedMessages = Array.isArray(messages) ? messages : [messages];

      // Format messages for KafkaJS
      const formattedMessages = normalizedMessages.map(msg => {
        if (typeof msg === 'object' && (msg.key !== undefined || msg.value !== undefined)) {
          return {
            key: msg.key ? this.serializeValue(msg.key) : null,
            value: this.serializeValue(msg.value),
            headers: msg.headers || {},
            partition: msg.partition,
            timestamp: msg.timestamp || Date.now().toString(),
          };
        } else {
          return {
            key: null,
            value: this.serializeValue(msg),
            timestamp: Date.now().toString(),
          };
        }
      });

      const result = await this.producer.send({
        topic: targetTopic,
        messages: formattedMessages,
        acks: options.acks !== undefined ? options.acks : -1,
        timeout: options.timeout || 30000,
        compression: options.compression || 0,
      });

      console.log(`Successfully sent ${formattedMessages.length} message(s) to topic: ${targetTopic}`);
      return result;
    } catch (error) {
      console.error('Failed to send message:', error);
      throw error;
    }
  }

  async sendBatch(topicMessages, options = {}) {
    if (!this.isConnected) {
      throw new Error('Producer is not connected. Call connect() first.');
    }

    try {
      const batch = {
        topicMessages: topicMessages.map(tm => ({
          topic: tm.topic,
          messages: Array.isArray(tm.messages) ? tm.messages.map(msg => ({
            key: msg.key ? this.serializeValue(msg.key) : null,
            value: this.serializeValue(msg.value),
            headers: msg.headers || {},
            partition: msg.partition,
            timestamp: msg.timestamp || Date.now().toString(),
          })) : [{
            key: null,
            value: this.serializeValue(tm.messages),
            timestamp: Date.now().toString(),
          }],
        })),
        acks: options.acks !== undefined ? options.acks : -1,
        timeout: options.timeout || 30000,
      };

      const result = await this.producer.sendBatch(batch);
      console.log('Successfully sent batch messages');
      return result;
    } catch (error) {
      console.error('Failed to send batch messages:', error);
      throw error;
    }
  }

  serializeValue(value) {
    if (value === null || value === undefined) {
      return null;
    }

    if (Buffer.isBuffer(value)) {
      return value;
    }

    if (typeof value === 'string') {
      return value;
    }

    if (typeof value === 'object') {
      return JSON.stringify(value);
    }

    return String(value);
  }

  async transaction(callback) {
    if (!this.isConnected) {
      throw new Error('Producer is not connected. Call connect() first.');
    }

    const transaction = await this.producer.transaction();

    try {
      await callback(transaction);
      await transaction.commit();
      console.log('Transaction committed successfully');
    } catch (error) {
      await transaction.abort();
      console.error('Transaction aborted:', error);
      throw error;
    }
  }

  on(eventName, callback) {
    this.producer.on(eventName, callback);
  }

  async getMetadata(topics = []) {
    try {
      const admin = this.kafka.admin();
      await admin.connect();
      const metadata = await admin.fetchTopicMetadata({ topics });
      await admin.disconnect();
      return metadata;
    } catch (error) {
      console.error('Failed to fetch metadata:', error);
      throw error;
    }
  }
}

module.exports = KafkaProducer;
