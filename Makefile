.PHONY: install format lint test up down clean help

# Configuration
PYTHON := .venv/bin/python
PIP := .venv/bin/pip
RUFF := .venv/bin/ruff
MYPY := .venv/bin/mypy
PYTEST := .venv/bin/pytest

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Create virtual environment and install all dependencies (dev + metrics)
	python3 -m venv .venv
	$(PIP) install -e ".[all,dev]"
	@echo "Virtual environment created and dependencies installed. Run 'source .venv/bin/activate'."

format: ## Auto-format code and fix simple linting issues using ruff
	$(RUFF) check --fix .
	$(RUFF) format .

lint: ## Run strict type checking (mypy) and linting (ruff)
	$(RUFF) check .
	$(MYPY) .

test: ## Run the full test suite (requires docker for integration tests)
	$(PYTEST) -v

test-integration: ## Run only integration tests
	$(PYTEST) tests/integration/ -v -m integration

up: ## Start the local infrastructure (Postgres + NATS) via Docker Compose
	docker compose up -d

down: ## Stop the local infrastructure and remove containers
	docker compose down

clean: ## Remove python cache files and virtual environment
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf .venv
	@echo "Cleaned up cache directories and virtual environment."
