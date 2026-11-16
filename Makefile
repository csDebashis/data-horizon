SHELL := /bin/bash
export PATH := /Applications/Docker.app/Contents/Resources/bin:$(PATH)
PYTHON ?= .venv/bin/python
DOCKER ?= docker
COMPOSE = "$(DOCKER)" compose
export PYTHONPATH := packages/core/src:services/api/src:services/collector/src:services/processor/src:tools/operations/src:.
DATA_MODE ?= live
export DATA_MODE
VCS_REF ?= $(shell git rev-parse HEAD)

.PHONY: setup bootstrap build up down status logs test unit integration browser frontend-check production-check production-up backup restore kafdrop-up kafdrop-stop kafdrop-logs kafka-ui-up kafka-ui-stop preflight

# First use: generate keys, build the images and initialise private Docker files.
setup: bootstrap build
	$(PYTHON) scripts/upload_runtime.py

bootstrap:
	$(PYTHON) scripts/bootstrap.py

build:
	$(PYTHON) scripts/retain_release.py check-build --source-commit "$(VCS_REF)"
	$(COMPOSE) build --build-arg VCS_REF="$(VCS_REF)" api collector processor runtime-init dashboard postgres

# Normal start uses already built images. It does not reset or rebuild data.
up:
	$(COMPOSE) up -d --no-build --wait --wait-timeout 240

down:
	$(COMPOSE) down

status:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --tail 100

preflight:
	$(PYTHON) scripts/preflight.py --output reports/preflight.json

unit:
	mkdir -p reports/raw
	DATA_MODE=synthetic $(PYTHON) -m pytest -m 'not integration and not browser and not performance and not live and not recovery and not spark' --junitxml=reports/raw/unit.xml

test: unit

integration:
	DATA_MODE=synthetic $(PYTHON) scripts/run_integration.py

browser:
	mkdir -p reports/raw
	RUN_BROWSER=1 $(PYTHON) -m pytest tests/e2e --junitxml=reports/raw/browser.xml

frontend-check:
	cd frontend && npm run lint && npm run typecheck && npm test && npm run build

kafdrop-up:
	$(COMPOSE) --profile kafdrop up -d --no-deps --no-build --wait --wait-timeout 180 kafdrop

kafdrop-stop:
	$(COMPOSE) --profile kafdrop stop kafdrop

kafdrop-logs:
	$(COMPOSE) --profile kafdrop logs --tail 100 kafdrop

kafka-ui-up:
	$(COMPOSE) --profile kafka-ui up -d --no-deps --wait --wait-timeout 180 kafka-ui

kafka-ui-stop:
	$(COMPOSE) --profile kafka-ui stop kafka-ui

production-check:
	$(PYTHON) scripts/production_check.py

production-up: production-check
	$(COMPOSE) -f compose.yaml -f compose.production.yaml build postgres proxy
	$(COMPOSE) -f compose.yaml -f compose.production.yaml up -d --no-build --wait --wait-timeout 300

backup:
	$(PYTHON) scripts/backup.py backup --output "$(OUTPUT)"

restore:
	$(PYTHON) scripts/backup.py restore --input "$(INPUT)" --confirm-database "$(CONFIRM_DATABASE)"
