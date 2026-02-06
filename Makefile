.PHONY: install test lint format load upload validate clean deploy dev 

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
		uv run modal secret create aws-asos --force \
			AWS_ACCESS_KEY_ID="$$AWS_ACCESS_KEY_ID" \
			AWS_SECRET_ACCESS_KEY="$$AWS_SECRET_ACCESS_KEY" \
			AWS_DEFAULT_REGION="$${AWS_DEFAULT_REGION:-us-east-1}" \
			S3_BUCKET="$$S3_BUCKET" \
			$${S3_PREFIX:+S3_PREFIX="$$S3_PREFIX"} \
			$${AWS_ENDPOINT_URL:+AWS_ENDPOINT_URL="$$AWS_ENDPOINT_URL"}
	@echo "Deploying to Modal..."
	uv run modal deploy modal_app.py

# Validate data for gaps and quality issues
validate:
	uv run python scripts/validate.py $(if $(YEAR),--year $(YEAR)) $(if $(UPDATE),--update-progress) --verbose $(if $(filter global,$(NETWORKS)),--global)

# Maintenance
clean:
	rm -rf .pytest_cache __pycache__ .ruff_cache .coverage data/ logs/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# Development
dev:
	@echo "Serving viewer at http://localhost:3000/viewer.html"
	uv run python -m http.server 3000
