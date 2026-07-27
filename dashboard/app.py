"""Live demo dashboard: streams real PaySim rows through the real
Kubernetes pipeline (Kafka -> flink-worker -> Feast) and scores each
one via the deployed API's `/predict`, showing the prediction next to
PaySim's own ground-truth `isFraud` label plus running metrics.

Reuses this project's own wire-format code (`domain.schemas`,
`streaming.serializer`) rather than reimplementing it, and reads
through `data.ingestion.load_paysim_csv` for the same schema
validation every other entry point gets.

Prerequisites (see dashboard/README.md):
    kubectl port-forward -n fraud-detection svc/kafka 9094:9094

Run:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st
from confluent_kafka import Producer
from sklearn.metrics import confusion_matrix, precision_recall_fscore_support

from fraud_detection.data.ingestion import load_paysim_csv
from fraud_detection.domain.schemas import transaction_from_dict
from fraud_detection.streaming.serializer import serialize_transaction

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "paysim_k8s_sample.csv"
DEFAULT_API_URL = "http://localhost:8090"
DEFAULT_TOPIC = "transactions"
DEFAULT_BOOTSTRAP = "localhost:9094"  # kafka.yaml's EXTERNAL listener, not 9092 (in-cluster only)
BATCH_PER_RERUN = 3  # transactions processed per Streamlit rerun while streaming
HISTORY_LIMIT = 500  # cap so a long-running session doesn't grow memory unbounded

st.set_page_config(page_title="Fraud Detection — Live Stream", layout="wide")


@st.cache_data
def load_data(csv_path: str) -> pd.DataFrame:
    return load_paysim_csv(csv_path)


def init_state() -> None:
    st.session_state.setdefault("history", [])
    st.session_state.setdefault("row_index", 0)
    st.session_state.setdefault("producer", None)
    st.session_state.setdefault("producer_bootstrap", None)


def get_producer(bootstrap_servers: str) -> Producer:
    """Recreates the cached Producer if `bootstrap_servers` changed.

    A real bug, found running this live: caching the Producer keyed
    only on "does one exist yet" meant changing the sidebar's Kafka
    bootstrap-servers field (or its default, while iterating on this
    file) silently kept publishing through the *old* connection —
    confirmed via rdkafka logs still resolving a stale broker address
    minutes after the field had visibly changed.
    """
    if (
        st.session_state.producer is None
        or st.session_state.producer_bootstrap != bootstrap_servers
    ):
        st.session_state.producer = Producer({"bootstrap.servers": bootstrap_servers})
        st.session_state.producer_bootstrap = bootstrap_servers
    return st.session_state.producer


def row_to_record(row: pd.Series) -> dict[str, Any]:
    """Cast a PaySim CSV row to plain-Python, JSON-serializable types.

    `df.iloc[idx]` values are numpy scalars (int64/float64) — fine for
    `transaction_from_dict` (it casts internally) but not for
    `requests`' `json=`, which uses stdlib `json.dumps` and can't
    serialize numpy scalars. One explicit cast here avoids that
    surfacing later as a confusing request failure.
    """
    return {
        "step": int(row["step"]),
        "type": str(row["type"]),
        "amount": float(row["amount"]),
        "nameOrig": str(row["nameOrig"]),
        "oldbalanceOrg": float(row["oldbalanceOrg"]),
        "newbalanceOrig": float(row["newbalanceOrig"]),
        "nameDest": str(row["nameDest"]),
        "oldbalanceDest": float(row["oldbalanceDest"]),
        "newbalanceDest": float(row["newbalanceDest"]),
        "isFlaggedFraud": int(row["isFlaggedFraud"]),
    }


def stream_one(
    df: pd.DataFrame,
    bootstrap_servers: str,
    topic: str,
    api_url: str,
    settle_seconds: float,
) -> dict[str, Any]:
    """Publish one real row to Kafka, give flink-worker a moment to push
    its features into Feast, then call `/predict` on that same
    transaction — the same live path verified by hand against this
    deployment (producer -> Kafka -> flink-worker -> Feast/Redis -> API).
    """
    idx = st.session_state.row_index % len(df)
    st.session_state.row_index += 1

    row = df.iloc[idx]
    ground_truth = int(row["isFraud"])
    record = row_to_record(row)

    transaction = transaction_from_dict(record)
    producer = get_producer(bootstrap_servers)
    producer.produce(topic, value=serialize_transaction(transaction))
    producer.poll(0)
    producer.flush(5)

    time.sleep(settle_seconds)

    result: dict[str, Any] = {
        "row": idx,
        "nameOrig": record["nameOrig"],
        "nameDest": record["nameDest"],
        "type": record["type"],
        "amount": record["amount"],
        "ground_truth": ground_truth,
        "prediction": None,
        "fraud_probability": None,
        "latency_ms": None,
        "correct": None,
        "error": None,
    }
    try:
        resp = requests.post(f"{api_url}/predict", json=record, timeout=5)
        if resp.status_code == 200:
            body = resp.json()
            result["prediction"] = body["prediction"]
            result["fraud_probability"] = round(body["fraud_probability"], 6)
            result["latency_ms"] = round(body["latency_ms"], 2)
            result["correct"] = body["prediction"] == ground_truth
        else:
            result["error"] = resp.json().get("detail", resp.text)
    except requests.RequestException as exc:
        result["error"] = str(exc)

    return result


def render_metrics(history: list[dict[str, Any]]) -> None:
    scored = [r for r in history if r["prediction"] is not None]
    if not scored:
        st.info("No successful predictions yet — check 'Run live stream' in the sidebar.")
        return

    y_true = [r["ground_truth"] for r in scored]
    y_pred = [r["prediction"] for r in scored]
    accuracy = sum(1 for t, p in zip(y_true, y_pred, strict=True) if t == p) / len(scored)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average="binary", zero_division=0
    )

    cols = st.columns(5)
    cols[0].metric("Scored", len(scored))
    cols[1].metric("Accuracy", f"{accuracy:.1%}")
    cols[2].metric("Precision", f"{precision:.1%}")
    cols[3].metric("Recall", f"{recall:.1%}")
    cols[4].metric("F1", f"{f1:.1%}")

    left, right = st.columns(2)
    with left:
        st.subheader("Confusion matrix")
        cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
        st.dataframe(
            pd.DataFrame(
                cm,
                index=["Actual: legit", "Actual: fraud"],
                columns=["Pred: legit", "Pred: fraud"],
            ),
            width="stretch",
        )
    with right:
        st.subheader("Rolling accuracy (last 50)")
        window = min(50, len(scored))
        rolling = (
            pd.Series([1 if t == p else 0 for t, p in zip(y_true, y_pred, strict=True)])
            .rolling(window=window, min_periods=1)
            .mean()
        )
        st.line_chart(rolling)


def main() -> None:
    init_state()

    st.title("Fraud Detection — Live Stream")
    st.caption(
        "Streams real PaySim transactions through Kafka -> flink-worker -> Feast, "
        "scores each one via the deployed API's `/predict`, and compares against "
        "PaySim's own ground-truth `isFraud` label."
    )

    with st.sidebar:
        st.header("Settings")
        api_url = st.text_input("API URL", DEFAULT_API_URL)
        bootstrap_servers = st.text_input("Kafka bootstrap servers", DEFAULT_BOOTSTRAP)
        topic = st.text_input("Kafka topic", DEFAULT_TOPIC)
        settle_seconds = st.slider("Settle time after publish (s)", 0.5, 5.0, 1.5, 0.5)
        st.caption(
            "Needs `kubectl port-forward -n fraud-detection svc/kafka 9094:9094` "
            "running separately — see dashboard/README.md."
        )
        running = st.checkbox("Run live stream", value=False)
        if st.button("Reset history"):
            st.session_state.history = []
            st.session_state.row_index = 0

    df = load_data(str(DEFAULT_CSV_PATH))

    if running:
        for _ in range(BATCH_PER_RERUN):
            result = stream_one(df, bootstrap_servers, topic, api_url, settle_seconds)
            st.session_state.history.append(result)
        st.session_state.history = st.session_state.history[-HISTORY_LIMIT:]

    render_metrics(st.session_state.history)

    st.subheader("Recent transactions")
    recent = list(reversed(st.session_state.history[-25:]))
    if recent:
        st.dataframe(pd.DataFrame(recent), width="stretch")
    else:
        st.info("Check 'Run live stream' in the sidebar to start.")

    if running:
        st.rerun()


if __name__ == "__main__":
    main()
