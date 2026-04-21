IMAGE := options-tracker:latest
COMPOSE := docker compose

.PHONY: build run stop logs clean rebuild help news-test

help:
	@echo "Targets:"
	@echo "  build     - Build the Docker image"
	@echo "  run       - Run the app (foreground, Ctrl-C to stop)"
	@echo "  stop      - Stop and remove the container"
	@echo "  logs      - Tail container logs"
	@echo "  rebuild   - Force a no-cache rebuild"
	@echo "  clean     - Stop container and remove the image"
	@echo "  news-test - Smoke-test Yahoo Finance news (SYMS=\"AAPL NVDA\")"

build:
	$(COMPOSE) build

run:
	@test -f .env || (echo "ERROR: .env missing. Run: cp .env.example .env and fill it in." && exit 1)
	@mkdir -p .cache
	$(COMPOSE) up

stop:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

rebuild:
	$(COMPOSE) build --no-cache

clean: stop
	-docker image rm $(IMAGE)

SYMS ?= AAPL NVDA TSLA
news-test:
	@if [ -x .venv/bin/python ]; then \
		.venv/bin/python scripts/test_news.py $(SYMS); \
	else \
		docker run --rm -v "$(PWD)":/app -w /app $(IMAGE) \
			python scripts/test_news.py $(SYMS); \
	fi

