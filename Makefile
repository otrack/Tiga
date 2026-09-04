IMAGE_NAME := 0track/tiga-suite
PROTOCOL   ?= tiga

.PHONY: all build jni up down restart clean logs

all: build

build:
	docker build -t $(IMAGE_NAME) .

jni:
	mvn install -f ycsb_jni/pom.xml -q

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
