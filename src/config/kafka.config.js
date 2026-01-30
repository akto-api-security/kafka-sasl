require('dotenv').config();

class KafkaConfig {
  constructor(customConfig = {}) {
    this.config = {
      brokers: customConfig.brokers || process.env.KAFKA_BROKERS?.split(',') || ['localhost:9092'],
      clientId: customConfig.clientId || process.env.KAFKA_CLIENT_ID || 'kafka-producer-client',
      sasl: this.getSaslConfig(customConfig),
      ssl: this.getSslConfig(customConfig),
      connectionTimeout: customConfig.connectionTimeout || parseInt(process.env.KAFKA_CONNECTION_TIMEOUT) || 30000,
      requestTimeout: customConfig.requestTimeout || parseInt(process.env.KAFKA_REQUEST_TIMEOUT) || 30000,
      retry: {
        retries: customConfig.retries || parseInt(process.env.KAFKA_PRODUCER_RETRY_ATTEMPTS) || 5,
        initialRetryTime: customConfig.initialRetryTime || parseInt(process.env.KAFKA_PRODUCER_RETRY_DELAY) || 300,
      },
      logLevel: customConfig.logLevel || process.env.LOG_LEVEL || 'info',
    };
  }

  getSaslConfig(customConfig) {
    const saslEnabled = customConfig.saslEnabled !== undefined
      ? customConfig.saslEnabled
      : process.env.KAFKA_SASL_ENABLED === 'true';

    if (!saslEnabled) {
      return null;
    }

    const mechanism = customConfig.saslMechanism || process.env.KAFKA_SASL_MECHANISM || 'plain';

    const saslConfig = {
      mechanism,
      username: customConfig.saslUsername || process.env.KAFKA_SASL_USERNAME,
      password: customConfig.saslPassword || process.env.KAFKA_SASL_PASSWORD,
    };

    // Add additional SASL mechanisms support
    if (mechanism === 'scram-sha-256' || mechanism === 'scram-sha-512') {
      saslConfig.mechanism = mechanism;
    } else if (mechanism === 'aws') {
      saslConfig.authorizationIdentity = customConfig.authorizationIdentity || process.env.KAFKA_AWS_AUTHORIZATION_IDENTITY;
      saslConfig.accessKeyId = customConfig.accessKeyId || process.env.KAFKA_AWS_ACCESS_KEY_ID;
      saslConfig.secretAccessKey = customConfig.secretAccessKey || process.env.KAFKA_AWS_SECRET_ACCESS_KEY;
      saslConfig.sessionToken = customConfig.sessionToken || process.env.KAFKA_AWS_SESSION_TOKEN;
    }

    return saslConfig;
  }

  getSslConfig(customConfig) {
    const sslEnabled = customConfig.sslEnabled !== undefined
      ? customConfig.sslEnabled
      : process.env.KAFKA_SSL_ENABLED === 'true';

    if (!sslEnabled) {
      return null;
    }

    return {
      rejectUnauthorized: customConfig.rejectUnauthorized !== undefined
        ? customConfig.rejectUnauthorized
        : process.env.KAFKA_SSL_REJECT_UNAUTHORIZED !== 'false',
      ca: customConfig.ca || process.env.KAFKA_SSL_CA,
      key: customConfig.key || process.env.KAFKA_SSL_KEY,
      cert: customConfig.cert || process.env.KAFKA_SSL_CERT,
    };
  }

  getConfig() {
    // Remove null values
    const config = { ...this.config };
    if (!config.sasl) delete config.sasl;
    if (!config.ssl) delete config.ssl;
    return config;
  }
}

module.exports = KafkaConfig;
