# Quick Start Guide

Three ways to run the Kafka Producer API:

---

## Option 1: Minikube (Kubernetes) ⭐ Recommended

**Prerequisites:** Kafka running in Minikube

### Deploy
```bash
./deploy.sh
```

### Get URL
```bash
minikube service kafka-producer-api --url
# Output: http://127.0.0.1:XXXXX
```

### Test
```bash
URL=$(minikube service kafka-producer-api --url)
curl $URL/health
curl -X POST $URL/api/publish -H "Content-Type: application/json" -d '{"topic":"test-topic","message":"Hello"}'
```

### Update After Code Changes
```bash
# Bump version in deployment.yaml (v3 -> v4)
docker build --no-cache -t kafka-producer-api:v4 .
minikube image load kafka-producer-api:v4
# Edit k8s/deployment.yaml: change image tag to v4
kubectl apply -f k8s/deployment.yaml
```

---

## Option 2: Local Machine (No Docker)

**Prerequisites:**
- Node.js installed
- Kafka accessible (use port-forward if in Kubernetes)

### Setup Port-Forward (if Kafka is in Kubernetes)
```bash
kubectl port-forward svc/kafka-sasl 9092:9092
```

### Configure
Edit `.env`:
```env
KAFKA_BROKERS=localhost:9092
KAFKA_SASL_USERNAME=kafka-client
KAFKA_SASL_PASSWORD=client-secret
```

### Run
```bash
npm install
npm start
```

### Test
```bash
curl http://localhost:3000/health
curl -X POST http://localhost:3000/api/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"test-topic","message":"Hello"}'
```

### Stop
```
Ctrl+C
```

---

## Option 3: Docker (Local Container)

**Prerequisites:** Docker installed

### Build
```bash
docker build -t kafka-producer-api:local .
```

### Run
```bash
# If Kafka is in Kubernetes
kubectl port-forward svc/kafka-sasl 9092:9092

# Run container (in another terminal)
docker run --rm -p 3000:3000 \
  -e KAFKA_BROKERS=host.docker.internal:9092 \
  -e KAFKA_SASL_USERNAME=kafka-client \
  -e KAFKA_SASL_PASSWORD=client-secret \
  kafka-producer-api:local
```

### Test
```bash
curl http://localhost:3000/health
curl -X POST http://localhost:3000/api/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"test-topic","message":"Hello"}'
```

### Stop
```
Ctrl+C
```

---

## Comparison

| Method | Pros | Cons | Best For |
|--------|------|------|----------|
| **Minikube** | Production-like, auto-restart, service discovery | Requires K8s knowledge | Production simulation |
| **Local Machine** | Fast iteration, easy debugging | Manual start/stop | Development |
| **Docker** | Isolated environment, consistent | Extra build step | Testing |

---

## Common Commands

### View Logs
```bash
# Minikube
kubectl logs -l app=kafka-producer-api -f

# Local/Docker
# Logs appear in terminal
```

### Check Kafka Connection
```bash
# All methods
curl http://localhost:3000/health
# or
curl $(minikube service kafka-producer-api --url)/health
```

### Publish Test Message
```bash
# Replace URL with your endpoint
curl -X POST http://localhost:3000/api/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"test-topic","message":"Test message"}'
```

---

## Troubleshooting

### "Connection refused" or "ECONNREFUSED"
- **Minikube:** Check pod is running: `kubectl get pods -l app=kafka-producer-api`
- **Local:** Ensure port-forward is running: `kubectl port-forward svc/kafka-sasl 9092:9092`
- **Docker:** Use `host.docker.internal` instead of `localhost`

### "Authentication failed"
- Verify credentials in `.env` match Kafka configuration
- Check Kafka JAAS config: `kubectl exec -it <kafka-pod> -- cat /etc/kafka/secrets/jaas.conf`

### "kafka-sasl not found" (Local only)
- Using Kubernetes Kafka? Must use port-forward to localhost:9092
- Or add to `/etc/hosts`: `echo "127.0.0.1 kafka-sasl" | sudo tee -a /etc/hosts`

---

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_BROKERS` | Kafka broker addresses | `localhost:9092` or `kafka-sasl:9092` |
| `KAFKA_SASL_USERNAME` | SASL username | `kafka-client` |
| `KAFKA_SASL_PASSWORD` | SASL password | `client-secret` |
| `KAFKA_SASL_ENABLED` | Enable SASL auth | `true` |
| `KAFKA_DEFAULT_TOPIC` | Default topic | `test-topic` |
| `PORT` | API server port | `3000` |

---

## Next Steps

1. ✅ Choose your deployment method
2. ✅ Start the API
3. ✅ Test with curl or Postman
4. ✅ Check README.md for full API documentation

**API Docs:** `GET /api/docs`
