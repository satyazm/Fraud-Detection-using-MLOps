# kubernetes/

Manifests for deploying this system to a real local `kind` cluster —
see the README's "Kubernetes deployment" section for setup and
ADR-0009 for the decisions and real bugs behind them.

`kind-cluster.yaml` is kind's own cluster config (`kind create cluster
--config`), not a `kubectl apply`-able resource — everything else here
is.
