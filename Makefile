.PHONY: install test lint format backfill upload update validate query clean serve help

# Default target
help:
	@echo "ASOS Geoparquet - Available Commands"
	@echo "====================================="
	@echo ""
	@echo "Setup:"
	@echo "  make install          Install dependencies"
	@echo ""
	@echo "Data:"
	@echo "  make backfill         Backfill historical data to local parquet files"
	@echo "  make update           Incremental update (for hourly cron)"
	@echo "  make upload           Upload local data to S3 (configure script first)"
	@echo "  make validate         Validate data for gaps and quality issues"
	@echo "  make query            Run example queries against data"
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
	@echo "  make backfill START=2000-01-01"
	@echo "  make backfill STATES=CA,TX START=2024-01-01"
	@echo "  make backfill CHUNK_MONTHS=36  # 3-year chunks (fewer API calls)"
	@echo "  make backfill CHUNK_MONTHS=12  # 1-year chunks (less memory)"
	@echo "  make update LOOKBACK=6         # Fetch last 6 hours"
	@echo "  make validate YEAR=2023        # Validate specific year"

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
CHUNK_MONTHS ?= 24

# Backfill to local parquet files
# Logs are automatically saved to logs/backfill-{timestamp}.log
backfill:
	uv run python scripts/backfill.py $(if $(START),--start $(START)) $(if $(END),--end $(END)) $(if $(STATES),--states $(STATES)) --chunk-months $(CHUNK_MONTHS)

# Incremental update for hourly cron
# Logs are automatically saved to logs/update-{timestamp}.log
LOOKBACK ?= 2
update:
	uv run python scripts/update.py --lookback $(LOOKBACK) $(if $(STATES),--states $(STATES))

# Upload local data to S3 (configure scripts/upload_s3.sh first)
upload:
	./scripts/upload_s3.sh

# Validate data for gaps and quality issues
YEAR ?=
validate:
ifdef YEAR
	uv run python scripts/validate.py --year $(YEAR) --verbose
else
	uv run python scripts/validate.py --verbose
endif

# Run example queries
query:
	uv run python scripts/query_example.py

# Maintenance
clean:
	rm -rf .pytest_cache __pycache__ .ruff_cache .coverage data/ logs/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# Development
serve:
	@echo "Serving viewer at http://localhost:3000/viewer.html"
	uv run python -m http.server 3000
