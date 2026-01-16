# =============================================================================
# AWET Cloud Deployment Guide
# =============================================================================

## Overview

This guide covers deploying AWET to production cloud environments. The platform 
uses Kubernetes with Kustomize for configuration management.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Cloud Provider (AWS/GCP/Azure)               │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
│  │   Ingress    │────│   Grafana    │    │ Prometheus   │          │
│  │  Controller  │    │  Dashboard   │    │   Metrics    │          │
│  └──────────────┘    └──────────────┘    └──────────────┘          │
│         │                                                           │
│  ┌──────┴──────────────────────────────────────────────────┐       │
│  │                    Kubernetes Cluster                    │       │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐        │       │
│  │  │   Data     │  │  Feature   │  │ Prediction │        │       │
│  │  │ Ingestion  │──│Engineering │──│   (TFT)    │        │       │
│  │  └────────────┘  └────────────┘  └────────────┘        │       │
│  │         │                               │               │       │
│  │  ┌──────┴───────────────────────────────┘              │       │
│  │  │  Kafka (Confluent/MSK/Strimzi)                      │       │
│  │  └──────────────────────────────────────────────────────┘       │
│  │                                                          │       │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐        │       │
│  │  │    Risk    │──│ Execution  │──│ Watchtower │        │       │
│  │  │   Gate     │  │  (Paper)   │  │  Monitor   │        │       │
│  │  └────────────┘  └────────────┘  └────────────┘        │       │
│  └──────────────────────────────────────────────────────────┘       │
│         │                                                           │
│  ┌──────┴──────────────────────────────────────────────────┐       │
│  │                    Data Layer                            │       │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐        │       │
│  │  │TimescaleDB │  │   Redis    │  │  S3/GCS    │        │       │
│  │  │  (Audit)   │  │  (Cache)   │  │  (Models)  │        │       │
│  │  └────────────┘  └────────────┘  └────────────┘        │       │
│  └──────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Kubernetes cluster (1.28+)
- kubectl configured
- Kustomize (built into kubectl)
- Container registry access (GHCR)

### Deploy to Dev

```bash
# Preview what will be deployed
kubectl kustomize deploy/k8s/overlays/dev

# Apply to cluster
kubectl apply -k deploy/k8s/overlays/dev

# Watch rollout
kubectl rollout status deployment -n awet-dev -w
```

### Deploy to Production

```bash
# Preview
kubectl kustomize deploy/k8s/overlays/prod

# Apply
kubectl apply -k deploy/k8s/overlays/prod

# Verify
kubectl get pods -n awet-prod
```

## Cloud Provider Options

### AWS (Recommended)

**Services:**
- EKS (Kubernetes)
- MSK (Managed Kafka)
- RDS (TimescaleDB via PostgreSQL)
- ElastiCache (Redis)
- S3 (Model storage)
- ECR (Container registry)

**Setup:**
```bash
# Create EKS cluster
eksctl create cluster \
  --name awet-prod \
  --region us-east-1 \
  --nodegroup-name workers \
  --node-type m5.large \
  --nodes 3 \
  --nodes-min 2 \
  --nodes-max 5

# Configure kubectl
aws eks update-kubeconfig --name awet-prod --region us-east-1
```

**Estimated Monthly Cost (Production):**
| Service | Spec | Cost |
|---------|------|------|
| EKS Cluster | Control plane | $73 |
| EC2 (3x m5.large) | Workers | ~$210 |
| MSK (kafka.m5.large x3) | Kafka | ~$450 |
| RDS (db.t3.medium) | TimescaleDB | ~$50 |
| ElastiCache (cache.t3.micro) | Redis | ~$15 |
| S3 | Model storage | ~$5 |
| **Total** | | **~$800/mo** |

### GCP

**Services:**
- GKE (Kubernetes)
- Confluent Cloud or self-managed Kafka
- Cloud SQL (PostgreSQL + TimescaleDB extension)
- Memorystore (Redis)
- GCS (Model storage)
- Artifact Registry

**Setup:**
```bash
# Create GKE cluster
gcloud container clusters create awet-prod \
  --region us-central1 \
  --machine-type e2-standard-2 \
  --num-nodes 3 \
  --enable-autoscaling \
  --min-nodes 2 \
  --max-nodes 5

# Get credentials
gcloud container clusters get-credentials awet-prod --region us-central1
```

### Azure

**Services:**
- AKS (Kubernetes)
- Event Hubs or Confluent Cloud
- Azure Database for PostgreSQL
- Azure Cache for Redis
- Blob Storage (Models)
- ACR (Container registry)

## Infrastructure as Code (Terraform)

For reproducible infrastructure, use Terraform:

```hcl
# deploy/terraform/main.tf
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 19.0"

  cluster_name    = "awet-prod"
  cluster_version = "1.28"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    workers = {
      min_size     = 2
      max_size     = 5
      desired_size = 3
      instance_types = ["m5.large"]
    }
  }
}
```

## Secrets Management

### Option 1: External Secrets Operator (Recommended)

```bash
# Install ESO
helm repo add external-secrets https://charts.external-secrets.io
helm install external-secrets external-secrets/external-secrets -n external-secrets --create-namespace

# Create ClusterSecretStore for AWS
kubectl apply -f - <<EOF
apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  name: aws-secretsmanager
spec:
  provider:
    aws:
      service: SecretsManager
      region: us-east-1
      auth:
        jwt:
          serviceAccountRef:
            name: external-secrets
            namespace: external-secrets
EOF
```

### Option 2: Sealed Secrets

```bash
# Install Sealed Secrets controller
kubectl apply -f https://github.com/bitnami-labs/sealed-secrets/releases/download/v0.24.0/controller.yaml

# Seal a secret
kubeseal --format=yaml < secrets.yaml > sealed-secrets.yaml
```

### Option 3: HashiCorp Vault

```bash
# Install Vault with Helm
helm repo add hashicorp https://helm.releases.hashicorp.com
helm install vault hashicorp/vault --set "server.dev.enabled=true"
```

## Monitoring Setup

### Prometheus + Grafana

```bash
# Install kube-prometheus-stack
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --set grafana.adminPassword=admin
```

### ServiceMonitor for AWET

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: awet-services
  namespace: awet-prod
spec:
  selector:
    matchLabels:
      app.kubernetes.io/part-of: awet
  endpoints:
    - port: http
      path: /metrics
      interval: 30s
```

## CI/CD Pipeline

The GitHub Actions workflow (`.github/workflows/ci.yml`) handles:

1. **Lint & Test** - On every PR
2. **Build Images** - On push to main/develop
3. **Deploy Dev** - Automatic on develop branch
4. **Deploy Prod** - Manual approval required

### Required Secrets

Configure in GitHub repository settings:

| Secret | Description |
|--------|-------------|
| `KUBE_CONFIG_DEV` | Base64 kubeconfig for dev cluster |
| `KUBE_CONFIG_PROD` | Base64 kubeconfig for prod cluster |

```bash
# Generate kubeconfig secret
kubectl config view --minify --raw | base64 -w0
```

## Safety Checklist

Before going to production:

- [ ] **TRADING_MODE=paper** enforced at all levels
- [ ] Execution agent requires approval token
- [ ] Secrets stored in external secret manager
- [ ] Network policies restrict pod-to-pod traffic
- [ ] Resource limits set for all containers
- [ ] Pod disruption budgets configured
- [ ] Backup strategy for TimescaleDB
- [ ] Alerting configured for:
  - [ ] Consumer lag > threshold
  - [ ] Error rate spike
  - [ ] Pod restarts
  - [ ] Memory pressure
- [ ] Audit trail writing to durable storage
- [ ] Log aggregation configured (Loki/ELK/CloudWatch)

## Rollback Procedure

```bash
# View deployment history
kubectl rollout history deployment/awet-prediction -n awet-prod

# Rollback to previous version
kubectl rollout undo deployment/awet-prediction -n awet-prod

# Rollback to specific revision
kubectl rollout undo deployment/awet-prediction -n awet-prod --to-revision=2
```

## Scaling

```bash
# Manual scaling
kubectl scale deployment awet-feature-engineering -n awet-prod --replicas=4

# HPA (Horizontal Pod Autoscaler)
kubectl apply -f - <<EOF
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: awet-prediction-hpa
  namespace: awet-prod
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: awet-prediction
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
EOF
```

## Troubleshooting

```bash
# Check pod status
kubectl get pods -n awet-prod -o wide

# View logs
kubectl logs -f deployment/awet-prediction -n awet-prod

# Describe pod for events
kubectl describe pod <pod-name> -n awet-prod

# Exec into container
kubectl exec -it deployment/awet-prediction -n awet-prod -- /bin/bash

# Check Kafka consumer lag
kubectl exec -n awet-prod deployment/awet-watchtower -- \
  curl -s http://localhost:8000/metrics | grep kafka_consumer_lag
```
