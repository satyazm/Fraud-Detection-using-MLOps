.PHONY: install install-dev lint format typecheck test precommit clean \
	ingest validate preprocess train evaluate \
	kafka-up kafka-down redis-up redis-down infra-up infra-down flink-jar \
	producer consumer feast-apply materialize flink-worker api

PYTHON := python3.11

# apache-flink (PyFlink) needs setuptools<81 present *before* it builds
# (apache-beam's setup.py imports pkg_resources) and must be installed
# with --no-build-isolation so that build can see it. See requirements/base.txt.
install:
	$(PYTHON) -m pip install "setuptools<81"
	$(PYTHON) -m pip install --no-build-isolation -r requirements/prod.txt

install-dev:
	$(PYTHON) -m pip install "setuptools<81"
	$(PYTHON) -m pip install --no-build-isolation -r requirements/dev.txt
	$(PYTHON) -m pip install --no-build-isolation -e .
	pre-commit install

lint:
	ruff check src tests feast_repo
	black --check src tests feast_repo

format:
	ruff check --fix src tests feast_repo
	black src tests feast_repo

typecheck:
	mypy src

test:
	pytest --cov=src

precommit:
	pre-commit run --all-files

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage htmlcov

# --- Milestone 2: data pipeline ---
ingest:
	fraud-detection ingest

validate:
	fraud-detection validate

preprocess:
	fraud-detection preprocess

# --- Milestone 3: model training ---
train:
	fraud-detection train

evaluate:
	fraud-detection evaluate

# --- Milestone 4: Kafka streaming ---
kafka-up:
	docker compose up -d kafka kafka-ui

kafka-down:
	docker compose down kafka kafka-ui

producer:
	fraud-detection producer

consumer:
	fraud-detection consumer

# --- Milestone 5: real-time feature platform (Feast + Redis + Flink) ---
redis-up:
	docker compose up -d redis

redis-down:
	docker compose down redis

infra-up: kafka-up redis-up

infra-down:
	docker compose down kafka kafka-ui redis

# One-time download: the Flink<->Kafka connector JAR isn't a pip
# package, PyFlink loads it via env.add_jars() at job-submission time.
flink-jar:
	mkdir -p .flink-jars
	test -f .flink-jars/flink-sql-connector-kafka-5.0.0-2.2.jar || \
		curl -sL -o .flink-jars/flink-sql-connector-kafka-5.0.0-2.2.jar \
		https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar

feast-apply:
	fraud-detection feast-apply

materialize:
	fraud-detection materialize

flink-worker: flink-jar
	fraud-detection flink-worker

# --- Placeholder operational commands (wired up to real logic in later milestones) ---
api:
	fraud-detection api
