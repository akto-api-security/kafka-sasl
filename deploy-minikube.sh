#!/bin/bash

set -e

echo "🐳 Building Docker image..."
docker build -t kafka-producer-api:latest .

echo "📦 Loading image into Minikube..."
minikube image load kafka-producer-api:latest

echo "🚀 Deploying to Kubernetes..."
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

echo "⏳ Waiting for deployment to be ready..."
kubectl rollout status deployment/kafka-producer-api --timeout=120s

echo "✅ Deployment complete!"
echo ""
echo "📍 Service Information:"
kubectl get svc kafka-producer-api

echo ""
echo "🌐 Get the API URL:"
echo "   minikube service kafka-producer-api --url"
echo ""
echo "🔍 Check pod status:"
echo "   kubectl get pods -l app=kafka-producer-api"
echo ""
echo "📋 View logs:"
echo "   kubectl logs -l app=kafka-producer-api -f"
echo ""
echo "🧪 Test API:"
echo "   curl \$(minikube service kafka-producer-api --url)/health"
