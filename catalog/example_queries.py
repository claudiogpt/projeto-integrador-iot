"""Consultas de exemplo contra o catálogo — uma por padrão de acesso.

    python catalog/example_queries.py

Cada consulta abaixo existe porque um componente da plataforma precisa dela.
É esse mapeamento (padrão de acesso → consulta → índice) que justifica o
modelo de documento escolhido; ver docs/modelagem_nosql.md.

Para as consultas críticas o script imprime o estágio vencedor do explain(),
evidenciando IXSCAN em vez de COLLSCAN.
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def winning_stage(explain: dict) -> str:
    stage = explain["queryPlanner"]["winningPlan"]
    names = []
    while stage:
        names.append(stage.get("stage", "?"))
        stage = stage.get("inputStage") or (stage.get("queryPlan") if "queryPlan" in stage else None)
    return " → ".join(names)


def q1_enrich_reading(database, sensor_id: str) -> None:
    """P1 — Enriquecer uma leitura: do sensor_id para a máquina e a linha.

    É o acesso mais frequente de toda a plataforma: toda leitura que sobe para
    Silver precisa resolver o sensor. Resolvido por índice multikey, com uma
    leitura e nenhum join.
    """
    filtro = {"sensors.sensor_id": sensor_id}
    projecao = {"line_id": 1, "line.name": 1, "model": 1,
                "sensors": {"$elemMatch": {"sensor_id": sensor_id}}}
    doc = database.machines.find_one(filtro, projecao)
    print("\nP1 — enriquecimento de leitura", f"({sensor_id})")
    print("   ", doc)
    print("    plano:", winning_stage(database.machines.find(filtro, projecao).explain()))


def q2_machines_of_line(database, line_id: str) -> None:
    """P2 — Máquinas ativas de uma linha: base das agregações por linha (Gold)."""
    filtro = {"line_id": line_id, "status": "active"}
    docs = list(database.machines.find(filtro, {"_id": 1, "model": 1}))
    print("\nP2 — máquinas ativas da linha", line_id)
    print("   ", [d["_id"] for d in docs])
    print("    plano:", winning_stage(database.machines.find(filtro).explain()))


def q3_thresholds(database) -> None:
    """P3 — Tabela de limiares por sensor: alimenta o detector da Entrega 3."""
    pipeline = [
        {"$unwind": "$sensors"},
        {"$match": {"sensors.status": "active"}},
        {"$project": {"_id": 0, "sensor_id": "$sensors.sensor_id",
                      "sensor_type": "$sensors.sensor_type",
                      "min": "$sensors.alert_threshold.min",
                      "max": "$sensors.alert_threshold.max"}},
        {"$sort": {"sensor_id": 1}},
        {"$limit": 5},
    ]
    print("\nP3 — limiares de alerta (5 primeiros de",
          database.machines.count_documents({}) * 4, "sensores)")
    for row in database.machines.aggregate(pipeline):
        print("   ", row)


def q4_fleet_summary(database) -> None:
    """P4 — Inventário por linha e tipo de sensor: usado no dashboard (Entrega 2)."""
    pipeline = [
        {"$unwind": "$sensors"},
        {"$group": {"_id": {"line": "$line_id", "tipo": "$sensors.sensor_type"},
                    "sensores": {"$sum": 1},
                    "maquinas": {"$addToSet": "$_id"}}},
        {"$project": {"_id": 0, "linha": "$_id.line", "tipo": "$_id.tipo",
                      "sensores": 1, "maquinas": {"$size": "$maquinas"}}},
        {"$sort": {"linha": 1, "tipo": 1}},
    ]
    print("\nP4 — inventário por linha e tipo de sensor")
    for row in database.machines.aggregate(pipeline):
        print("   ", row)


def q5_rejects_validator(database) -> None:
    """P5 — O validador $jsonSchema barra um documento fora do padrão."""
    print("\nP5 — validador do catálogo")
    try:
        database.machines.insert_one({"_id": "MAQUINA-X", "line_id": "L-01",
                                      "status": "active", "sensors": []})
        print("    ERRO: o validador deixou passar um documento inválido")
    except Exception as error:  # pymongo.errors.WriteError
        print("    documento inválido rejeitado, como esperado:",
              type(error).__name__)


def main() -> None:
    load_dotenv()
    client = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=10_000)
    database = client[os.environ.get("MONGO_DB", "acmetech")]
    if database.machines.count_documents({}) == 0:
        raise SystemExit("catálogo vazio — rode antes: python catalog/seed_catalog.py")

    amostra = database.machines.find_one({}, {"sensors.sensor_id": 1, "line_id": 1})
    q1_enrich_reading(database, amostra["sensors"][0]["sensor_id"])
    q2_machines_of_line(database, amostra["line_id"])
    q3_thresholds(database)
    q4_fleet_summary(database)
    q5_rejects_validator(database)


if __name__ == "__main__":
    main()
