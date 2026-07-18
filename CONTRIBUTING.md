# Contributing to nats-outbox

First off, thank you for considering contributing to `nats-outbox`! It's people like you that make the open-source community such an amazing place to learn, inspire, and create.

This document provides guidelines and instructions for contributing to this project.

## 1. Development Environment Setup

The project uses modern Python packaging (`pyproject.toml`) and requires Python 3.11+.

### Clone the repository
```bash
git clone https://github.com/your-username/nats-outbox.git
cd nats-outbox
```

### Create a virtual environment and install dependencies
We recommend using the provided Makefile which automates the setup. The `make install` command will create a `.venv` and install all tools required for testing and linting.

```bash
make install
source .venv/bin/activate
```

## 2. Infrastructure for Local Testing

The integration tests and the local examples require a running PostgreSQL database and a NATS JetStream server. 

You can easily spin them up using the Makefile:
```bash
make up
```

*Note: The test suite uses `testcontainers` and will spin up its own isolated Docker containers during tests. You do not strictly need the local docker-compose stack running just to run the tests, but it is required to run the `fastapi_app.py` example.*

## 3. Code Quality and Linting

We maintain strict code quality standards to ensure the project remains robust and maintainable.

### Formatting & Linting
We use `ruff` as our primary linter and formatter, and `mypy` for static type checking.

```bash
# Auto-fix linting errors and format code
make format

# Run strict type checking and linting
make lint
```

## 4. Running Tests

We use `pytest` for our test suite. The integration tests rely on Docker being available on your system.

```bash
# Run all tests
make test

# Run only integration tests
make test-integration
```

If you are adding a new feature, please include tests that cover the new functionality. If you are fixing a bug, please include a regression test to ensure the bug is not reintroduced.

## 5. Pull Request Process

1. **Fork** the repository and create your branch from `main`.
2. **Implement** your changes.
3. **Verify** your changes by running the tests and linters (`ruff check .`, `mypy .`, `pytest`).
4. **Commit** your changes with clear, descriptive commit messages.
5. **Open a Pull Request** describing the problem you are solving or the feature you are adding.

## 6. Architecture & Roadmap

Before contributing large architectural changes, please check our roadmap in `README.md` to understand the current technical direction (e.g., the upcoming V2 Logical Replication Relay). 

If you want to work on a major feature, consider opening an issue first to discuss the design with the maintainers.

---
Thank you for your contributions! 
