.PHONY: install test test-unit test-integration test-cov lint format validate backfill worker worker-hourly compact clean help

# Default target
help:
	@echo "ASOS Geoparquet - Available Commands"
	@echo "====================================="
	@echo ""
	@echo "Setup:"
	@echo "  make install          Install dependencies"
	@echo ""
	@echo "Testing:"
	@echo "  make test             Run all tests"
	@echo "  make test-unit        Run unit tests only"
	@echo "  make test-integration Run integration tests (requires network)"
	@echo "  make test-cov         Run tests with coverage report"
	@echo ""
	@echo "Linting:"
	@echo "  make lint             Check code with ruff"
	@echo "  make format           Format code with ruff"
	@echo ""
	@echo "Single-File Mode (legacy):"
	@echo "  make validate         Validate existing geoparquet"
	@echo "  make backfill         Run full historical backfill"
	@echo "  make backfill-ca      Quick test (1 week CA data)"
	@echo "  make worker           Update worker (append to single file)"
	@echo ""
	@echo "Partitioned Mode (recommended for cloud/hourly updates):"
	@echo "  make worker-hourly    Hourly update worker (partitioned)"
	@echo "  make compact          Compact old partitions into single files"
	@echo "  make dataset-info     Show partitioned dataset info"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean            Remove generated files"
	@echo ""
	@echo "Examples:"
	@echo "  make backfill STATES=CA,TX START=2024-01-01"
	@echo "  make worker LOOKBACK=48"
	@echo "  make worker-hourly STATES=CA LOOKBACK=2"
	@echo "  make compact OLDER_THAN=7"

# Setup
install:
	uv sync --all-extras

# Testing
test:
	uv run pytest

test-unit:
	uv run pytest tests/test_validation.py -v

test-integration:
	uv run pytest tests/test_integration.py -v

test-cov:
	uv run pytest --cov=asos_parquet --cov-report=term-missing

# Linting
lint:
	uv run ruff check src/ scripts/ tests/

format:
	uv run ruff format src/ scripts/ tests/
	uv run ruff check --fix src/ scripts/ tests/

# Variables
STATES ?=
START ?=
END ?=
LOOKBACK ?= 24
LOOKBACK_HOURLY ?= 2
OLDER_THAN ?= 1
FILE ?= data/asos.parquet
DATASET ?= data/asos

# Single-file operations (legacy)
validate:
	uv run python scripts/validate.py $(FILE)

backfill:
ifdef STATES
ifdef START
	uv run python scripts/backfill.py --states $(STATES) --start $(START) $(if $(END),--end $(END))
else
	uv run python scripts/backfill.py --states $(STATES)
endif
else
ifdef START
	uv run python scripts/backfill.py --start $(START) $(if $(END),--end $(END))
else
	uv run python scripts/backfill.py
endif
endif

backfill-ca:
	uv run python scripts/backfill.py --states CA --start 2024-12-01 --end 2024-12-07

backfill-resume:
	uv run python scripts/backfill.py --resume

worker:
	uv run python scripts/worker.py --lookback $(LOOKBACK)

# Partitioned operations (recommended)
worker-hourly:
ifdef STATES
	uv run python scripts/worker_partitioned.py --states $(STATES) --lookback $(LOOKBACK_HOURLY)
else
	uv run python scripts/worker_partitioned.py --lookback $(LOOKBACK_HOURLY)
endif

compact:
	uv run python scripts/compact.py --older-than $(OLDER_THAN)

dataset-info:
	@uv run python -c "from asos_parquet.partitioned import get_dataset_info; import json; print(json.dumps(get_dataset_info(), indent=2))"

# Maintenance
clean:
	rm -rf .pytest_cache __pycache__ .ruff_cache .coverage
	rm -rf src/asos_parquet/__pycache__ tests/__pycache__
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

clean-data:
	rm -rf data/asos data/asos.parquet data/backfill_checkpoint.json
