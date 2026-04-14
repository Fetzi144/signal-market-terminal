.PHONY: dev setup stop logs migrate reset frontend-install frontend-test frontend-build frontend-validate secrets-scan

dev: setup
	docker compose up

setup:
	@test -f backend/.env || (cp backend/.env.example backend/.env && echo "Created backend/.env from .env.example - edit to add API keys")
	@test -f backend/.env && echo ".env ready"

stop:
	docker compose down

logs:
	docker compose logs -f backend

migrate:
	docker compose exec backend alembic upgrade head

reset:
	docker compose down -v
	docker compose up

frontend-install:
	npm --prefix frontend install

frontend-test:
	npm --prefix frontend test

frontend-build:
	npm --prefix frontend run build

frontend-validate: frontend-test frontend-build

secrets-scan:
	python scripts/scan_secrets.py
