"""FastAPI real-time inference service (Milestone 6).

Transaction -> Feast online features (Redis) -> MLflow Production model
-> fraud probability -> JSON response. Named `api`, not `serving` (the
name `docs/architecture.md` originally reserved for this layer) —
renamed for this milestone to match the package path actually built;
see docs/decisions/0007-fastapi-inference-service.md.
"""
