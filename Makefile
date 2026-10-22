# Shortcuts for the whole pipeline. `make help` lists them.
# MONTH can be YYYYMM or latest:  make trips MONTH=202509
MONTH ?= latest
RUN = docker compose run --rm app

.PHONY: help up migrate stations streaming trips dbt export dashboard airflow showcase showcase-lite test status down

help:
	@grep -E '^[a-z]+:.*## ' Makefile | sed 's/:.*## /\t/'

up: ## start Postgres
	docker compose up -d --wait postgres

migrate: up ## apply every sql/0*.sql (safe to rerun)
	@for f in sql/0*.sql; do echo "applying $$f"; docker exec -i dockwatch-db psql -q -U dockwatch -d dockwatch < $$f; done

stations: migrate ## load station names (Step 5)
	$(RUN) python load_stations.py

streaming: migrate ## start Redpanda + producer + consumer (Steps 7, 8)
	docker compose --profile streaming up -d --build

trips: migrate ## download + load one month of trips (Steps 9, 10)
	$(RUN) python trips.py --month $(MONTH)

dbt: ## build and test every dbt model (Step 12)
	$(RUN) dbt build --project-dir dbt --profiles-dir dbt

export: ## write docs/data.json for the project page
	$(RUN) python export_page_data.py

dashboard: ## start the dashboard on port 8501 (Step 13)
	docker compose --profile dashboard up -d --build dashboard

airflow: ## start Airflow on port 8081, login admin / admin (Step 11)
	docker compose --profile airflow up -d --build airflow

showcase: ## everything, end to end, then refresh the page data
	docker compose build app
	$(MAKE) stations
	$(MAKE) streaming
	$(MAKE) trips MONTH=$(MONTH)
	@echo "letting the live stream collect for 2 minutes..."; sleep 120
	$(MAKE) dbt
	$(MAKE) export
	@echo ""
	@echo "Done. Publish the page data with:"
	@echo "  git add docs/data.json && git commit -m 'Refresh page data' && git push"

showcase-lite: ## same result as showcase, but downloads run from your terminal (for when containers have no internet)
	pip install -q -r requirements.txt
	$(MAKE) migrate
	python load_stations.py
	python trips.py --month $(MONTH)
	$(MAKE) dbt
	python export_page_data.py
	@echo ""
	@echo "Done. Publish the page data with:"
	@echo "  git add docs/data.json && git commit -m 'Real data' && git push"

test: ## unit tests
	python -m pytest -q tests

status: ## what's running
	docker compose --profile streaming --profile dashboard --profile airflow ps

down: ## stop everything (data is kept)
	docker compose --profile streaming --profile dashboard --profile airflow --profile tools down
