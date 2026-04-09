.PHONY: dev setup stop logs migrate reset

dev: setup
	docker compose up

setup:
	@test -f backend/.env || (cp backend/.env.example backend/.env && echo "Created backend/.env from .env.example — edit to add API keys")
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
