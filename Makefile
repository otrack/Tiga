IMAGE_NAME := 0track/tiga-suite
PROTOCOL   ?= tiga

.PHONY: all build up down restart clean logs

all: build

build:
	docker build -t $(IMAGE_NAME) .

up:
	docker compose up -d

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

clean: down
	docker rmi $(IMAGE_NAME) 2>/dev/null || true
	bazel clean 2>/dev/null || true
