"""Gerador de leituras sintéticas de sensores da AcmeTech.

Emite leituras no contrato contracts/sensor_reading.v2.schema.json como JSON
Lines (uma leitura por linha) em stdout.

    python simulator/simulator.py --machines 6 --count 1000 --seed 42 > readings.jsonl

Duas fontes de "problema" são geradas, e elas são deliberadamente distintas:

  * ANOMALIAS (--anomaly-rate): leituras VÁLIDAS no contrato mas com
    comportamento anormal. São o alvo do detector de anomalias (Entrega 3) e
    vêm marcadas com injected_anomaly/anomaly_type, o gabarito.

  * REGISTROS INVÁLIDOS (--invalid-rate): linhas que VIOLAM o contrato. São o
    alvo da validação de qualidade: a ingestão deve rejeitá-las e mandá-las
    para a quarentena, nunca para a Bronze.

Anomalias são episódios com estado, não eventos isolados: deriva e sensor
travado duram várias leituras do mesmo sensor. Por isso --anomaly-rate é a
probabilidade de INICIAR um episódio, não a fração de leituras anômalas.

A fração observada é bem maior, e vale a relação:

    fração_anômala ≈ (taxa × duração_média) / (1 + taxa × duração_média)

Com as durações padrão (média de ~23 leituras por episódio), --anomaly-rate
0.002 produz ~4% de leituras anômalas e 0.02 produz ~30%. O --stats imprime a
fração real de cada execução.
"""

import argparse
import json
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

SCHEMA_VERSION = 2


@dataclass(frozen=True)
class SensorProfile:
    code: str
    unit: str
    mean: float
    stddev: float


SENSOR_PROFILES = {
    "temperature": SensorProfile("TMP", "celsius", 65.0, 3.0),
    "vibration": SensorProfile("VIB", "mm_s", 4.5, 0.8),
    "pressure": SensorProfile("PRS", "bar", 6.0, 0.4),
    "power": SensorProfile("PWR", "kw", 22.0, 2.5),
}

# Episódios de anomalia: (duração mínima, duração máxima) em leituras do sensor.
ANOMALY_KINDS = {
    "spike": (1, 3),     # pico abrupto, curto (ex.: superaquecimento momentâneo)
    "drift": (20, 60),   # deriva lenta: o valor sobe/desce progressivamente
    "stuck": (15, 40),   # sensor travado: repete exatamente o último valor
}


@dataclass
class Episode:
    """Estado de um episódio de anomalia em curso, por sensor."""

    kind: str
    remaining: int
    total: int
    direction: int = 1
    frozen_value: float | None = None
    magnitude: float = 1.0
    elapsed: int = field(default=0)


def build_fleet(machines: int, lines: int) -> list[tuple[str, str]]:
    """Pares (line_id, machine_id), distribuindo as máquinas entre as linhas."""
    return [
        (f"L-{(index % lines) + 1:02d}", f"M-{index + 1:03d}")
        for index in range(machines)
    ]


def sensor_id_of(machine_id: str, sensor_type: str) -> str:
    return f"{machine_id}-{SENSOR_PROFILES[sensor_type].code}"


def normal_reading(
    line_id: str,
    machine_id: str,
    sensor_type: str,
    event_time: datetime,
    rng: random.Random,
) -> dict:
    profile = SENSOR_PROFILES[sensor_type]
    return {
        "schema_version": SCHEMA_VERSION,
        "reading_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "line_id": line_id,
        "machine_id": machine_id,
        "sensor_id": sensor_id_of(machine_id, sensor_type),
        "sensor_type": sensor_type,
        "value": round(rng.gauss(profile.mean, profile.stddev), 3),
        "unit": profile.unit,
        "event_time": event_time.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "injected_anomaly": False,
    }


def start_episode(rng: random.Random) -> Episode:
    kind = rng.choice(list(ANOMALY_KINDS))
    low, high = ANOMALY_KINDS[kind]
    total = rng.randint(low, high)
    return Episode(
        kind=kind,
        remaining=total,
        total=total,
        direction=rng.choice((-1, 1)),
        magnitude=rng.uniform(4.0, 9.0),  # em desvios-padrão do sensor
    )


def inject_anomaly(reading: dict, episode: Episode, rng: random.Random) -> dict:
    """Distorce a leitura conforme o episódio em curso e marca o gabarito.

    O valor continua válido no contrato (número, unidade correta): uma anomalia
    é um dado plausível porém anormal, não um dado quebrado.
    """
    profile = SENSOR_PROFILES[reading["sensor_type"]]
    episode.elapsed += 1

    if episode.kind == "spike":
        offset = episode.direction * episode.magnitude * profile.stddev
        reading["value"] = round(reading["value"] + offset, 3)

    elif episode.kind == "drift":
        # rampa linear: o desvio cresce de ~0 até magnitude ao longo do episódio
        progress = episode.elapsed / episode.total
        offset = episode.direction * episode.magnitude * profile.stddev * progress
        reading["value"] = round(reading["value"] + offset, 3)

    elif episode.kind == "stuck":
        # sensor travado: repete exatamente o mesmo valor durante todo o episódio
        if episode.frozen_value is None:
            episode.frozen_value = reading["value"]
        reading["value"] = episode.frozen_value

    reading["injected_anomaly"] = True
    reading["anomaly_type"] = episode.kind
    episode.remaining -= 1
    return reading


# --- Registros inválidos (violações de contrato, para exercitar a quarentena) ---

def _drop_required(reading: dict, rng: random.Random) -> dict:
    del reading["unit"]
    return reading


def _bad_enum(reading: dict, rng: random.Random) -> dict:
    reading["sensor_type"] = "humidity"
    return reading


def _bad_pattern(reading: dict, rng: random.Random) -> dict:
    reading["machine_id"] = "MAQ-1"
    return reading


def _wrong_type(reading: dict, rng: random.Random) -> dict:
    reading["value"] = f"{reading['value']} C"
    return reading


def _naive_timestamp(reading: dict, rng: random.Random) -> dict:
    reading["event_time"] = reading["event_time"].replace("Z", "")
    return reading


def _unknown_field(reading: dict, rng: random.Random) -> dict:
    reading["operator_notes"] = "troca de turno"
    return reading


CORRUPTIONS = {
    "missing_required": _drop_required,
    "bad_enum": _bad_enum,
    "bad_pattern": _bad_pattern,
    "wrong_type": _wrong_type,
    "naive_timestamp": _naive_timestamp,
    "unknown_field": _unknown_field,
    "broken_json": None,  # tratado na serialização
}


def corrupt(reading: dict, rng: random.Random) -> tuple[dict | None, str]:
    """Devolve (leitura corrompida | None, nome da corrupção).

    None significa "emita uma linha que nem é JSON válido".
    """
    kind = rng.choice(list(CORRUPTIONS))
    if kind == "broken_json":
        return None, kind
    return CORRUPTIONS[kind](reading, rng), kind


def generate(args: argparse.Namespace):
    """Gera leituras; cada item é (payload, rótulo).

    payload é um dict (leitura) ou None para linha propositalmente quebrada.
    """
    rng = random.Random(args.seed)
    fleet = build_fleet(args.machines, args.lines)
    event_time = datetime.now(timezone.utc) if args.start is None else args.start
    episodes: dict[str, Episode] = {}
    emitted = 0

    while args.count is None or emitted < args.count:
        for line_id, machine_id in fleet:
            for sensor_type in SENSOR_PROFILES:
                reading = normal_reading(line_id, machine_id, sensor_type, event_time, rng)
                key = reading["sensor_id"]

                episode = episodes.get(key)
                if episode is None and rng.random() < args.anomaly_rate:
                    episode = episodes[key] = start_episode(rng)
                if episode is not None:
                    reading = inject_anomaly(reading, episode, rng)
                    if episode.remaining <= 0:
                        del episodes[key]

                label = reading.get("anomaly_type", "normal")
                if rng.random() < args.invalid_rate:
                    reading, label = corrupt(reading, rng)

                yield reading, label
                emitted += 1
                if args.count is not None and emitted >= args.count:
                    return
        event_time += timedelta(seconds=args.interval)
        if args.realtime:
            time.sleep(args.interval)


def utc_datetime(text: str) -> datetime:
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def unit_interval(text: str) -> float:
    value = float(text)
    if not 0.0 <= value <= 1.0:
        raise argparse.ArgumentTypeError("informe um valor entre 0 e 1")
    return value


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--machines", type=int, default=6, help="tamanho do parque (M-001...)")
    parser.add_argument("--lines", type=int, default=2, help="linhas de produção (L-01...)")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="segundos entre duas leituras do mesmo sensor")
    parser.add_argument("--count", type=int, default=None,
                        help="para após N leituras (padrão: roda indefinidamente)")
    parser.add_argument("--anomaly-rate", type=unit_interval, default=0.01,
                        help="probabilidade de INICIAR um episódio de anomalia por leitura")
    parser.add_argument("--invalid-rate", type=unit_interval, default=0.0,
                        help="fração de linhas que violam o contrato (testa a quarentena)")
    parser.add_argument("--seed", type=int, default=None,
                        help="fixa os valores gerados; combine com --start para "
                             "uma execução idêntica byte a byte")
    parser.add_argument("--start", type=utc_datetime, default=None,
                        help="primeiro event_time (ISO 8601; sem fuso = UTC)")
    parser.add_argument("--realtime", action="store_true",
                        help="dorme --interval segundos entre as rodadas")
    parser.add_argument("--stats", action="store_true",
                        help="imprime um resumo por tipo de anomalia em stderr ao final")
    return parser.parse_args(argv)


def main(argv: list[str]) -> None:
    args = parse_args(argv)
    counters: dict[str, int] = {}
    for payload, label in generate(args):
        counters[label] = counters.get(label, 0) + 1
        if payload is None:
            sys.stdout.write('{"schema_version": 2, "reading_id": "truncado\n')
        else:
            sys.stdout.write(json.dumps(payload) + "\n")
        if args.realtime:
            sys.stdout.flush()
    if args.stats:
        total = sum(counters.values())
        print(f"total: {total}", file=sys.stderr)
        for label, count in sorted(counters.items()):
            print(f"  {label:18s} {count:7d}  ({count / total:.2%})", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1:])
