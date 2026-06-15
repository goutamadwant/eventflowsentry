.PHONY: test install install-dev build clean docker-up docker-down docker-benchmark benchmark sync-to-paper

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	pytest tests/ -v

build: clean
	python -m build

clean:
	rm -rf dist/ build/ *.egg-info src/*.egg-info

docker-up:
	docker compose -f docker/docker-compose.yml up -d

docker-down:
	docker compose -f docker/docker-compose.yml down -v

docker-benchmark:
	docker compose -f docker/docker-compose.yml build benchmark-runner
	docker compose -f docker/docker-compose.yml up -d kafka jobmanager taskmanager
	docker compose -f docker/docker-compose.yml run --rm --no-deps benchmark-runner

benchmark:
	python run_docker_benchmark.py

sync-to-paper:
	./sync_to_paper.sh
