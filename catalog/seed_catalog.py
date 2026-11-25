"""Cria e popula o catálogo de máquinas/sensores no MongoDB.

    python catalog/seed_catalog.py --machines 6 --lines 2

Decisões de modelagem (justificadas em docs/modelagem_nosql.md e ADR-0002):

  * Banco de DOCUMENTO: a unidade de acesso é a máquina com seus sensores,
    sempre lidos juntos. Um documento por máquina resolve o padrão de acesso
    dominante com uma única leitura, sem join.
  * Sensores EMBUTIDOS: cardinalidade baixa e limitada (4 por máquina), nunca
    consultados fora do contexto da máquina — o caso clássico de embedding.
  * Linha como REFERÊNCIA ESTENDIDA: guarda line_id + o nome da linha dentro
    da máquina, evitando um lookup só para exibir o nome, sem duplicar o
    restante dos dados da linha.
  * Faixas e limiares por sensor ficam no catálogo, e não no código: são eles
    que vão alimentar o detector de anomalias da Entrega 3.

O script é idempotente: pode rodar quantas vezes forem necessárias
(createCollection tolerante, collMod do validador, upserts e createIndex).
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import CollectionInvalid

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from simulator.simulator import SENSOR_PROFILES, build_fleet, sensor_id_of  # noqa: E402

# Faixa normal de operação e limiares de alerta, em múltiplos do desvio-padrão
# do perfil do sensor. Mantidos coerentes com o simulador de propósito: o
# detector da Entrega 3 será medido contra o gabarito que ele gera.
NORMAL_SIGMAS = 3.0
ALERT_SIGMAS = 4.5


def sensor_document(machine_id: str, sensor_type: str, interval_s: float) -> dict:
    profile = SENSOR_PROFILES[sensor_type]
    return {
        "sensor_id": sensor_id_of(machine_id, sensor_type),
        "sensor_type": sensor_type,
        "unit": profile.unit,
        "sampling_interval_s": interval_s,
        "normal_range": {
            "min": round(profile.mean - NORMAL_SIGMAS * profile.stddev, 3),
            "max": round(profile.mean + NORMAL_SIGMAS * profile.stddev, 3),
        },
        "alert_threshold": {
            "min": round(profile.mean - ALERT_SIGMAS * profile.stddev, 3),
            "max": round(profile.mean + ALERT_SIGMAS * profile.stddev, 3),
        },
        "status": "active",
    }


def machine_document(line_id: str, machine_id: str, interval_s: float) -> dict:
    index = int(machine_id.split("-")[1])
    return {
        "_id": machine_id,
        "line_id": line_id,
        # referência estendida: o nome da linha vem junto, o resto fica em `lines`
        "line": {"line_id": line_id, "name": f"Linha {line_id.split('-')[1]}"},
        "model": f"ACM-{700 + (index % 3) * 100}",
        "status": "active",
        "installed_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "sensors": [
            sensor_document(machine_id, sensor_type, interval_s)
            for sensor_type in SENSOR_PROFILES
        ],
    }


MACHINE_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "line_id", "status", "sensors"],
        "properties": {
            "_id": {"bsonType": "string", "pattern": "^M-[0-9]{3}$"},
            "line_id": {"bsonType": "string", "pattern": "^L-[0-9]{2}$"},
            "status": {"enum": ["active", "maintenance", "retired"]},
            "sensors": {
                "bsonType": "array",
                "minItems": 1,
                "items": {
                    "bsonType": "object",
                    "required": ["sensor_id", "sensor_type", "unit", "alert_threshold"],
                    "properties": {
                        "sensor_id": {"bsonType": "string",
                                      "pattern": "^M-[0-9]{3}-(TMP|VIB|PRS|PWR)$"},
                        "sensor_type": {
                            "enum": ["temperature", "vibration", "pressure", "power"]},
                        "unit": {"enum": ["celsius", "mm_s", "bar", "kw"]},
                        "status": {"enum": ["active", "faulty", "removed"]},
                    },
                },
            },
        },
    }
}


def ensure_collection(database, name: str, validator: dict | None = None) -> None:
    """Cria a coleção se não existir e (re)aplica o validador — idempotente."""
    try:
        database.create_collection(name)
    except CollectionInvalid:
        pass
    if validator is not None:
        database.command({"collMod": name, "validator": validator,
                          "validationLevel": "strict", "validationAction": "error"})


def seed(database, machines: int, lines: int, interval_s: float) -> dict:
    ensure_collection(database, "machines", MACHINE_VALIDATOR)
    ensure_collection(database, "lines")

    fleet = build_fleet(machines, lines)

    line_ops = [
        UpdateOne(
            {"_id": line_id},
            {"$set": {"name": f"Linha {line_id.split('-')[1]}", "plant": "AcmeTech BH"}},
            upsert=True,
        )
        for line_id in sorted({line_id for line_id, _ in fleet})
    ]
    machine_ops = [
        UpdateOne({"_id": machine_id},
                  {"$set": machine_document(line_id, machine_id, interval_s)},
                  upsert=True)
        for line_id, machine_id in fleet
    ]

    database.lines.bulk_write(line_ops, ordered=False)
    database.machines.bulk_write(machine_ops, ordered=False)

    # Índices escolhidos pelos padrões de acesso (ver docs/modelagem_nosql.md).
    database.machines.create_index([("sensors.sensor_id", ASCENDING)],
                                   name="ix_sensor_id", unique=True)
    database.machines.create_index([("line_id", ASCENDING), ("status", ASCENDING)],
                                   name="ix_line_status")
    database.machines.create_index([("sensors.sensor_type", ASCENDING)],
                                   name="ix_sensor_type")

    return {
        "lines": database.lines.count_documents({}),
        "machines": database.machines.count_documents({}),
        "sensors": next(database.machines.aggregate([
            {"$project": {"n": {"$size": "$sensors"}}},
            {"$group": {"_id": None, "total": {"$sum": "$n"}}},
        ]))["total"],
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--machines", type=int, default=6,
                        help="use o mesmo valor passado ao simulador")
    parser.add_argument("--lines", type=int, default=2,
                        help="use o mesmo valor passado ao simulador")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="intervalo nominal de amostragem, em segundos")
    return parser.parse_args(argv)


def main(argv: list[str]) -> None:
    load_dotenv()
    args = parse_args(argv)
    client = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=10_000)
    database = client[os.environ.get("MONGO_DB", "acmetech")]
    totals = seed(database, args.machines, args.lines, args.interval)
    print(f"catálogo pronto em {database.name}: "
          f"{totals['lines']} linhas, {totals['machines']} máquinas, "
          f"{totals['sensors']} sensores")
    print("índices:", ", ".join(sorted(database.machines.index_information())))


if __name__ == "__main__":
    main(sys.argv[1:])
