
DOCKER ?= docker
IMAGE ?= pdf2image-lambda:latest
CONTAINER ?= pdf2image-lambda-local
PORT ?= 9000
EVENT ?= {}

.PHONY: help build run shell invoke lint

help:
	@echo "build   Build the Lambda image"
	@echo "run     Run image locally on port $(PORT)"
	@echo "shell   Open a shell inside the image"
	@echo "invoke  Invoke the running Lambda with EVENT=<json or path>"
	@echo "lint    Run isort and flake8"

build:
	$(DOCKER) build -t $(IMAGE) .

run: build
	-$(DOCKER) rm -f $(CONTAINER)
	$(DOCKER) run -d --rm -p $(PORT):8080 --name $(CONTAINER) $(IMAGE)

shell: build
	$(DOCKER) run --rm -it --entrypoint /bin/bash $(IMAGE)

invoke:
	@if ! $(DOCKER) ps --format '{{.Names}}' | grep -q '^$(CONTAINER)$$'; then \
		echo "Container $(CONTAINER) is not running; start it first with 'make run'."; \
		exit 1; \
	fi
	@if [ -f "$(EVENT)" ]; then body="$$(cat "$(EVENT)")"; else body='$(EVENT)'; fi; \
	curl -s -X POST "http://localhost:$(PORT)/2015-03-31/functions/function/invocations" -d "$$body"

lint:
	isort .
	flake8 .
