DOCKER_RUN = docker run --rm -v $(PWD):/app -w /app --user $(shell id -u):$(shell id -g) --network common python:3.13.7-alpine
DOCKER_RUN_NO_NETWORK = docker run --rm -v $(PWD):/app -w /app --user $(shell id -u):$(shell id -g) python:3.13.7-alpine
VENV_BIN = .venv/bin
PYTHON = $(DOCKER_RUN) $(VENV_BIN)/python
ACTIVATE_VENV= source $(VENV_BIN)/activate
PIP = $(DOCKER_RUN) $(VENV_BIN)/pip

START ?= $(shell date +%Y-%m-01)
END ?= $(shell date +%Y-%m-%d)

up:
	docker compose up -d

down:
	docker compose down

install:
	$(DOCKER_RUN_NO_NETWORK) sh -c "python -m venv .venv && $(VENV_BIN)/pip install --upgrade pip && $(VENV_BIN)/pip install -r requirements.txt"

generate:
	$(PYTHON) generator.py

aggregate:
	# Пример: make aggregate START=2026-02-01 END=2026-03-01
	$(PYTHON) aggregator.py $(START) $(END)

init: up install
	@echo "Waiting for database to initialize..."
	sleep 10
	$(MAKE) generate
	@echo "Project is ready! Now you can run 'make aggregate START=... END=...'"
