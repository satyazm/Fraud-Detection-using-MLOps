.PHONY: install install-dev lint format typecheck test precommit clean ingest validate preprocess train producer consumer api

PYTHON := python3.11

install:
	$(PYTHON) -m pip install -r requirements/prod.txt

install-dev:
	$(PYTHON) -m pip install -r requirements/dev.txt
	$(PYTHON) -m pip install -e .
	pre-commit install

lint:
	ruff check src tests
	black --check src tests

format:
	ruff check --fix src tests
	black src tests

typecheck:
	mypy src

test:
	pytest --cov=src

precommit:
	pre-commit run --all-files

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage htmlcov

# --- Phase 2: data pipeline ---
ingest:
	fraud-detection ingest

validate:
	fraud-detection validate

preprocess:
	fraud-detection preprocess

# --- Placeholder operational commands (wired up to real logic in later phases) ---
train:
	fraud-detection train

producer:
	fraud-detection producer

consumer:
	fraud-detection consumer

api:
	fraud-detection api
