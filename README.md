# Simple Kafka Producer

One script to publish messages to Kafka with SASL authentication.

## Setup

```bash
npm install
cp .env.example .env
# Edit .env with your Kafka details
```

## Usage

```bash
# Publish default message
node publish.js

# Publish custom message
node publish.js "Your custom message"

# Or use npm
npm run publish "Hello Kafka"
```

## Configuration

Edit `.env`:

```env
KAFKA_BROKERS=localhost:9092
KAFKA_SASL_USERNAME=kafka-client
KAFKA_SASL_PASSWORD=client-secret
KAFKA_TOPIC=test-topic
```

## Connect to Kafka in Kubernetes

```bash
kubectl port-forward svc/kafka-sasl 9092:9092
```

Then run the script!

## That's it! 🚀

Just 3 files:
- `publish.js` - The script
- `.env` - Your config
- `package.json` - Dependencies
