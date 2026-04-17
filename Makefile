.PHONY: install test lint format load upload validate validate-prod clean deploy dev examples

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
UPDATE ?=
NETWORKS ?=
COUNTRIES ?=

# Load historical data year by year
# Progress is tracked in data/progress.json
# Logs are automatically saved to logs/load-{timestamp}.log
# With RESUME=1, current year does incremental update from last observation
load:
	uv run python scripts/load.py $(if $(YEAR),--year $(YEAR)) $(if $(START_YEAR),--start-year $(START_YEAR)) $(if $(RESUME),--resume) $(if $(NETWORKS),--networks $(NETWORKS)) $(if $(COUNTRIES),--countries $(COUNTRIES))

# Upload local data to S3 (configure scripts/upload_s3.sh first)
upload:
	./scripts/upload_s3.sh

# Deploy to Modal (reads credentials from .env)
# Creates/updates Modal secret and deploys the scheduled function
deploy:
	@if [ ! -f .env ]; then echo "Error: .env file not found. Copy .env.example and fill in values."; exit 1; fi
	@echo "Updating Modal secrets from .env..."
	@set -a && source .env && set +a && \
		uv run modal secret create source-coop-asos-s3 --force \
			ASOS_AWS_ACCESS_KEY_ID="$$ASOS_AWS_ACCESS_KEY_ID" \
			ASOS_AWS_SECRET_ACCESS_KEY="$$ASOS_AWS_SECRET_ACCESS_KEY" \
			ASOS_AWS_DEFAULT_REGION="$${ASOS_AWS_DEFAULT_REGION:-us-east-1}" \
			ASOS_S3_BUCKET="$$ASOS_S3_BUCKET" \
			$${ASOS_AWS_SESSION_TOKEN:+ASOS_AWS_SESSION_TOKEN="$$ASOS_AWS_SESSION_TOKEN"} \
			$${ASOS_S3_PREFIX:+ASOS_S3_PREFIX="$$ASOS_S3_PREFIX"} \
			$${ASOS_AWS_ENDPOINT_URL:+ASOS_AWS_ENDPOINT_URL="$$ASOS_AWS_ENDPOINT_URL"}
	@echo "Deploying to Modal..."
	uv run modal deploy modal_app.py

# Validate data for gaps and quality issues
validate:
	uv run python scripts/validate.py $(if $(YEAR),--year $(YEAR)) $(if $(UPDATE),--update-progress) --verbose $(if $(filter global,$(NETWORKS)),--global)

# Validate S3-hosted production data (reads S3_BUCKET/S3_PREFIX from .env)
validate-prod:
	uv run python scripts/validate_prod.py $(if $(YEAR),--year $(YEAR)) --verbose $(if $(filter global,$(NETWORKS)),--global) $(if $(SCHEMA_ONLY),--schema-only)

# Maintenance
clean:
	rm -rf .pytest_cache __pycache__ .ruff_cache .coverage data/ logs/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# Examples
examples:
	uv run python examples/station_history.py
	uv run python examples/coldest_temperature.py
	uv run python examples/wind_rose.py
	uv run python examples/summer_heatwave.py
	uv run python examples/precipitation_ranking.py

# Development
dev:
	@echo "Serving viewer at http://localhost:3000/viewer.html"
	uv run python -m http.server 3000
