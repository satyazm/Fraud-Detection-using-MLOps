"""Framework-agnostic business objects shared across every layer.

`domain` depends on nothing else in this package (not `streaming`, not
`serving`, not any ML library) so Kafka, FastAPI, and model choices can
all change without touching business logic. See
docs/decisions/0002-clean-architecture-layering.md.
"""
