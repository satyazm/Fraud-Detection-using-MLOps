# kubernetes/

Manifests for deploying this system to a local `kind` cluster — see the [Kubernetes guide](../docs/guides/kubernetes.md) for setup and [ADR-0009](../docs/decisions/0009-deployment-and-operations.md) for the decisions and real bugs behind them.

`kind-cluster.yaml` is kind's own cluster config (`kind create cluster --config`), not a `kubectl apply`-able resource — everything else here is.
