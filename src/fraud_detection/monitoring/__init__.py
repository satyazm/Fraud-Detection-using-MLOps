"""Observability (Milestone 7): Prometheus metrics, an Evidently AI
data-drift report, and the live-prediction log that report reads from.

Model *performance* monitoring (precision/recall/AP) is deliberately
not implemented here: this architecture has no ground-truth feedback
loop (nothing ever tells the system whether a served prediction was
right), so there's nothing honest to compute those from yet — see
docs/decisions/0008-monitoring.md.
"""
