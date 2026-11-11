# Kubernetes deployment & local infrastructure

Docker Compose uses the local `mlruns/` file store for MLflow; the
Kubernetes deployment below runs a real MLflow tracking server instead
(pods can't share a host filesystem — see ADR-0009).

## Kubernetes deployment

`kubernetes/` deploys the same system to a real local `kind` cluster —
`redis`, `kafka`, a real MLflow tracking server (`api`/`training-job`
point at it over HTTP, since K8s pods can't share a host filesystem
the way Compose's bind mount does), `api` (3 replicas, HPA-scaled
2-10 on CPU, reachable via an nginx `Ingress`), `flink-worker`, and
`prometheus`/`grafana`/`redis-exporter` mirroring the Compose
monitoring stack. See ADR-0009 for what actually broke running this
for real and how it was fixed.

One-time setup:

```bash
# Build the images kubernetes/*.yaml reference
docker build -f docker/Dockerfile.api -t mloops-api:latest .
docker build -f docker/Dockerfile.worker -t mloops-worker:latest .
docker build -f docker/Dockerfile.flink-worker -t mloops-flink-worker:latest .

# Create the cluster (run from the repo root — kind-cluster.yaml's
# extraMounts path is relative to the current working directory)
kind create cluster --config kubernetes/kind-cluster.yaml --name mloops

# Load the images into it — kind's own containerd doesn't see the
# host's Docker image store otherwise
kind load docker-image mloops-api:latest mloops-worker:latest mloops-flink-worker:latest --name mloops

# ingress-nginx and metrics-server aren't installed by kind by
# default; hpa.yaml/ingress.yaml need them
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

Deploy everything:

```bash
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/configmap.yaml -f kubernetes/secret.yaml
kubectl apply -f kubernetes/redis.yaml -f kubernetes/kafka.yaml -f kubernetes/mlflow.yaml
kubectl apply -f kubernetes/api.yaml -f kubernetes/flink-worker.yaml
kubectl apply -f kubernetes/prometheus.yaml -f kubernetes/grafana.yaml
kubectl apply -f kubernetes/jobs.yaml -f kubernetes/hpa.yaml -f kubernetes/ingress.yaml
```

`kind-cluster.yaml` configures `kind` itself and is not a Kubernetes
resource to apply.

`training-job` needs a model registered *and* promoted to
"Production" before `api` can serve real predictions (same one-time
step as Docker Compose's (see [Inference API](inference-api.md#promote-a-model-once)), run against this cluster's MLflow
instead):

```python
from mlflow.tracking import MlflowClient
MlflowClient(tracking_uri="http://localhost:5000").transition_model_version_stage(
    name="fraud-detection-classifier", version=1, stage="Production"
)
# (port-forward first: kubectl port-forward -n fraud-detection svc/mlflow 5000:5000)
```

Then reach the API via the ingress controller's mapped port (see
`kind-cluster.yaml`'s `extraPortMappings`): `curl http://localhost:8090/health`.

---

[← Back to the main README](../../README.md) · [Documentation index](../README.md)
