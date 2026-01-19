.PHONY: install test lint format load upload validate query clean serve help

# Default target
help:
	@echo "ASOS Geoparquet - Available Commands"
	@echo "====================================="
	@echo ""
	@echo "Setup:"
	@echo "  make install          Install dependencies"
	@echo ""
	@echo "Data:"
	@echo "  make load             Load historical data year by year (1940-present)"
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
	@echo "  make load YEAR=2023            # Load single year"
	@echo "  make load START_YEAR=2000      # Load from specific year"
	@echo "  make load RESUME=1             # Resume from progress.json"
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
YEAR ?=
START_YEAR ?=
RESUME ?=

# Load historical data year by year
# Progress is tracked in data/progress.json
# Logs are automatically saved to logs/load-{timestamp}.log
load:
	uv run python scripts/load.py $(if $(YEAR),--year $(YEAR)) $(if $(START_YEAR),--start-year $(START_YEAR)) $(if $(RESUME),--resume)

# Upload local data to S3 (configure scripts/upload_s3.sh first)
upload:
	./scripts/upload_s3.sh

# Validate data for gaps and quality issues
validate:
	uv run python scripts/validate.py $(if $(YEAR),--year $(YEAR)) --verbose

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
