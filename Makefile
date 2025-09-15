env:
	cp .env.example .env

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d


lint:
	uv sync --dev --extra lint
	uv run ruff check . --config pyproject.toml --diff
	uv run ruff format . --check  --config pyproject.toml --diff