# Atalhos para os comandos do README. Tudo aqui é opcional: cada alvo é um
# comando único, que pode ser copiado e colado.
.PHONY: help env up down clean seed queries simulate ingest test smoke

PY ?= .venv/bin/python
MACHINES ?= 6
LINES ?= 2
COUNT ?= 5000
INTERVAL ?= 60
SEED ?= 42
START ?= 2026-03-01T22:00:00
OUT ?= data/readings.jsonl

help:  ## lista os alvos disponíveis
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | expand -t22

env:  ## cria o virtualenv e instala as dependências
	python3.12 -m venv .venv && $(PY) -m pip install -q -r requirements.txt

up:  ## sobe a plataforma (object storage + catálogo + bucket)
	docker compose up -d --wait

down:  ## para a plataforma, preservando os dados
	docker compose down

clean:  ## para a plataforma e APAGA os volumes (lakehouse e catálogo)
	docker compose down -v

seed:  ## cria/atualiza o catálogo de máquinas e sensores no MongoDB
	$(PY) catalog/seed_catalog.py --machines $(MACHINES) --lines $(LINES) --interval $(INTERVAL)

queries:  ## roda as consultas de exemplo do catálogo
	$(PY) catalog/example_queries.py

simulate:  ## gera leituras sintéticas em $(OUT)
	@mkdir -p $(dir $(OUT))
	$(PY) simulator/simulator.py --machines $(MACHINES) --lines $(LINES) \
		--count $(COUNT) --interval $(INTERVAL) --seed $(SEED) --start $(START) \
		--anomaly-rate 0.002 --invalid-rate 0.01 --stats > $(OUT)

ingest:  ## ingere $(OUT) na camada Bronze
	$(PY) ingest/ingest_bronze.py $(OUT)

test:  ## testes unitários (não precisam de Docker)
	$(PY) -m pytest -q tests

smoke:  ## teste ponta a ponta contra a plataforma no ar
	./scripts/smoke_test.sh
