# Kafka Producer API with SASL Authentication

Modular Node.js Kafka producer with REST API, deployed in Kubernetes.

## 🚀 Quick Start

### Deploy to Minikube
```bash
./deploy-minikube.sh
```

### Get API URL
```bash
minikube service kafka-producer-api --url
# Output: http://127.0.0.1:52640
```

### Test
```bash
# Health check
curl http://127.0.0.1:52640/health

# Publish message
curl -X POST http://127.0.0.1:52640/api/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"test-topic","message":"Hello Kafka"}'
```

## 📡 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/docs` | API documentation |
| POST | `/api/publish` | Publish single message |
| POST | `/api/publish/bulk` | Publish multiple messages |
| POST | `/api/publish/batch` | Publish to multiple topics |
| GET | `/api/metadata` | Get Kafka metadata |

## 🔧 Configuration

**SASL Authentication:**
- Broker: `kafka-sasl:9092`
- Username: `kafka-client`
- Password: `client-secret`
- Mechanism: `PLAIN`

Configuration file: `.env`

## 📦 Project Structure

```
kafka-producer/
├── src/
│   ├── config/kafka.config.js    # Configuration management
│   ├── kafka/producer.js         # Producer class
│   ├── api-server.js             # Express API server
│   └── index.js                  # Module export
├── k8s/
│   ├── deployment.yaml           # Kubernetes deployment
│   └── service.yaml              # Kubernetes service
├── examples/                     # Usage examples
├── .env                          # Configuration
├── Dockerfile                    # Container image
├── deploy.sh                     # Deployment script
└── package.json
```

## 🧪 API Examples

### Simple Message
```bash
curl -X POST http://127.0.0.1:52640/api/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"test-topic","message":"Hello"}'
```

### Message with Key
```bash
curl -X POST http://127.0.0.1:52640/api/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"user-events","key":"user-123","message":{"userId":"123","event":"login"}}'
```

### Message with Headers
```bash
curl -X POST http://127.0.0.1:52640/api/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"orders","key":"order-456","message":{"orderId":"456","amount":99.99},"headers":{"source":"api","correlation-id":"abc-123"}}'
```

### Bulk Messages
```bash
curl -X POST http://127.0.0.1:52640/api/publish/bulk \
  -H "Content-Type: application/json" \
  -d '{"topic":"test-topic","messages":["Message 1","Message 2","Message 3"]}'
```

### Multiple Topics
```bash
curl -X POST http://127.0.0.1:52640/api/publish/batch \
  -H "Content-Type: application/json" \
  -d '{"topicMessages":[{"topic":"topic-1","messages":[{"key":"key1","value":"msg1"}]},{"topic":"topic-2","messages":[{"key":"key2","value":"msg2"}]}]}'
```

## 🔍 Kubernetes Commands

```bash
# Check pod status
kubectl get pods -l app=kafka-producer-api

# View logs
kubectl logs -l app=kafka-producer-api -f

# Get service info
kubectl get svc kafka-producer-api

# Restart deployment
kubectl rollout restart deployment/kafka-producer-api

# Delete deployment
kubectl delete -f k8s/
```

## 🔄 Update Deployment

After code changes:
```bash
./deploy-minikube.sh
```

## 📝 Use as Module

You can also import and use the producer directly:

```javascript
const KafkaProducer = require('./src/kafka/producer');

const producer = new KafkaProducer({
  brokers: ['kafka-sasl:9092'],
  saslEnabled: true,
  saslUsername: 'kafka-client',
  saslPassword: 'client-secret'
});

await producer.connect();
await producer.send('topic', { value: 'message' });
await producer.disconnect();
```

## 📊 Features

- ✅ SASL/PLAIN authentication
- ✅ REST API for easy integration
- ✅ Batch messaging support
- ✅ Custom headers and keys
- ✅ Auto-retry mechanism
- ✅ Kubernetes deployment
- ✅ Health checks
- ✅ Modular and reusable

## 🛠️ Development

Run locally (requires Kafka access):
```bash
npm install
npm start
```

Run examples:
```bash
node examples/simple-producer.js
```

## License

ISC
