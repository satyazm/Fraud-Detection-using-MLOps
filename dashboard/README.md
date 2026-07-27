# dashboard/

A live demo dashboard, not part of any deployed service — streams real
PaySim transactions through the actual Kubernetes pipeline (Kafka ->
`flink-worker` -> Feast/Redis) and scores each one via the deployed
API's `/predict`, showing the prediction next to PaySim's own
ground-truth `isFraud` label plus running accuracy/precision/recall/F1
and a confusion matrix.

## Run it

1. The Kubernetes deployment must already be up (`kubectl apply -f
   kubernetes/`) with `api` reachable via the ingress (`curl
   http://localhost:8090/health`).
2. Port-forward Kafka's `EXTERNAL` listener (port 9094, not 9092 —
   9092 is only advertised as `kafka:9092`, which the host can't
   resolve; see `kubernetes/kafka.yaml`) so the dashboard (running on
   the host) can publish to it — the API is already reachable via the
   ingress, but Kafka isn't exposed outside the cluster otherwise:
   ```bash
   kubectl port-forward -n fraud-detection svc/kafka 9094:9094
   ```
3. In another terminal:
   ```bash
   source .venv/bin/activate  # needs requirements/dev.txt installed
   streamlit run dashboard/app.py
   ```
4. Check "Run live stream" in the sidebar.

Each tick publishes one real row from `data/raw/paysim_k8s_sample.csv`
to Kafka, waits for `flink-worker` to push its features into Feast,
then calls `/predict` on that exact transaction — the same live path
verified by hand against this deployment, not a simulated one.
