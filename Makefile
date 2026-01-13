.PHONY: install test lint format backfill-r2 validate query clean serve help

# Default target
help:
	@echo "ASOS Geoparquet - Available Commands"
	@echo "====================================="
	@echo ""
	@echo "Setup:"
	@echo "  make install          Install dependencies"
	@echo ""
	@echo "Data:"
	@echo "  make backfill-r2      Backfill historical data to R2 (resumes automatically)"
	@echo "  make validate         Validate R2 data for gaps and quality issues"
	@echo "  make query            Run example queries against R2 data"
	@echo "  make clear-r2         Clear all data from R2 bucket"
	@echo ""
	@echo "Development:"
	@echo "  make serve            Serve viewer.html on localhost:3000"
	@echo "  make test             Run tests"
	@echo "  make lint             Check code with ruff"
	@echo "  make format           Format code with ruff"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean            Remove generated files"
	@echo ""
	@echo "Examples:"
	@echo "  make backfill-r2 START=2020-01-01"
	@echo "  make backfill-r2 STATES=CA,TX START=2024-01-01"
	@echo "  make backfill-r2 CHUNK_MONTHS=3  # Use more memory, run faster"
	@echo "  make validate YEAR=2023          # Validate specific year"

# Setup
install:
	uv sync --all-extras

# Testing & Linting
test:
	uv run pytest

lint:
	uv run ruff check src/ scripts/ tests/

format:
	uv run ruff format src/ scripts/ tests/
	uv run ruff check --fix src/ scripts/ tests/

# Variables
STATES ?=
START ?=
END ?=
CHUNK_MONTHS ?= 1

# R2 backfill (resumes automatically if no START specified)
# Logs are saved to logs/backfill-{timestamp}.log
backfill-r2:
	@mkdir -p logs
ifdef START
	uv run python scripts/backfill_r2.py --start $(START) $(if $(END),--end $(END)) $(if $(STATES),--states $(STATES)) --chunk-months $(CHUNK_MONTHS) 2>&1 | tee logs/backfill-$$(date +%Y%m%d-%H%M%S).log
else
	uv run python scripts/backfill_r2.py --resume --chunk-months $(CHUNK_MONTHS) 2>&1 | tee logs/backfill-$$(date +%Y%m%d-%H%M%S).log
endif

# Validate R2 data for gaps and quality issues
# Logs are saved to logs/validate-{timestamp}.log
YEAR ?=
validate:
	@mkdir -p logs
ifdef YEAR
	uv run python scripts/validate_r2.py --year $(YEAR) --verbose 2>&1 | tee logs/validate-$$(date +%Y%m%d-%H%M%S).log
else
	uv run python scripts/validate_r2.py --verbose 2>&1 | tee logs/validate-$$(date +%Y%m%d-%H%M%S).log
endif

# Run example queries
query:
	uv run python scripts/query_example.py

# Maintenance
clean:
	rm -rf .pytest_cache __pycache__ .ruff_cache .coverage data/ logs/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

clear-r2:
	@echo "Clearing all data from R2 bucket..."
	@bash -c 'source .env && AWS_ACCESS_KEY_ID=$$R2_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$$R2_SECRET_ACCESS_KEY aws s3 rm s3://dev/asos --recursive --endpoint-url https://$$R2_ACCOUNT_ID.r2.cloudflarestorage.com'

# Development
serve:
	@echo "Serving viewer at http://localhost:3000/viewer.html"
	uv run python -m http.server 3000
