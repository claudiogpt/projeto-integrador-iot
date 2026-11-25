"""Carrega a saída do simulador (JSON Lines) na camada Bronze.

    python ingest/ingest_bronze.py readings.jsonl [outro.jsonl ...]

Garantias desta ingestão (critério "Ingestão Bronze" do barema):

  * Parquet particionado por data do evento, layout Hive: bronze/sensor_readings/dt=YYYY-MM-DD/
  * Idempotente de verdade: reexecutar a mesma entrada não duplica nada, e
    ingerir um segundo arquivo do MESMO dia não apaga o que já estava lá.
    A estratégia é ler-mesclar-sobrescrever, deduplicando por reading_id
    (ver docs/adr/0003-idempotencia-bronze.md).
  * Registros que violam o contrato não entram na Bronze: vão para
    quarantine/sensor_readings/, com o motivo da rejeição.
  * O contrato é validado por versão (schema_version), suportando v1 e v2.
"""

import hashlib
import io
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv
from jsonschema import Draft202012Validator

CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"
BRONZE_PREFIX = "bronze/sensor_readings"
QUARANTINE_PREFIX = "quarantine/sensor_readings"

# Esquema explícito: o Parquet da Bronze não depende de inferência de tipos.
BRONZE_SCHEMA = pa.schema([
    pa.field("schema_version", pa.int32(), nullable=False),
    pa.field("reading_id", pa.string(), nullable=False),
    pa.field("line_id", pa.string(), nullable=False),
    pa.field("machine_id", pa.string(), nullable=False),
    pa.field("sensor_id", pa.string(), nullable=False),
    pa.field("sensor_type", pa.string(), nullable=False),
    pa.field("value", pa.float64(), nullable=False),
    pa.field("unit", pa.string(), nullable=False),
    pa.field("event_time", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("injected_anomaly", pa.bool_(), nullable=True),
    pa.field("anomaly_type", pa.string(), nullable=True),
    # Metadados de ingestão (linhagem) — não fazem parte do contrato do sensor.
    pa.field("ingested_at", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("source_file", pa.string(), nullable=False),
    pa.field("batch_id", pa.string(), nullable=False),
])


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        # O SeaweedFS não implementa os checksums novos do SDK AWS.
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            s3={"addressing_style": "path"},
        ),
    )


def ensure_bucket(client, bucket: str) -> None:
    existing = {entry["Name"] for entry in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)


def load_validators() -> dict[int, Draft202012Validator]:
    """Um validador por versão do contrato, descoberto pelos arquivos em contracts/."""
    validators: dict[int, Draft202012Validator] = {}
    for path in sorted(CONTRACTS_DIR.glob("sensor_reading.v*.schema.json")):
        schema = json.loads(path.read_text())
        version = schema["properties"]["schema_version"]["const"]
        validators[version] = Draft202012Validator(schema)
    if not validators:
        raise FileNotFoundError(f"nenhum contrato encontrado em {CONTRACTS_DIR}")
    return validators


def utc_event_date(reading: dict) -> str:
    """Data UTC (YYYY-MM-DD) do event_time; levanta ValueError se inválido."""
    event_time = datetime.fromisoformat(reading["event_time"])
    if event_time.tzinfo is None:
        raise ValueError("event_time must carry a UTC offset")
    return event_time.astimezone(timezone.utc).date().isoformat()


def batch_id_of(path: Path) -> str:
    """Identidade determinística do lote: hash do conteúdo do arquivo.

    O mesmo arquivo ingerido duas vezes produz o mesmo batch_id, o que torna a
    chave da quarentena estável e a reexecução visivelmente idempotente.
    """
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return digest[:16]


def read_readings(path: Path, validators: dict[int, Draft202012Validator]):
    """Separa o arquivo em leituras válidas e rejeitadas.

    Devolve (válidas, rejeitadas), onde cada rejeitada é
    {"line": n, "error": "...", "raw": "linha original"}.
    """
    valid: list[dict] = []
    rejected: list[dict] = []
    with path.open() as source:
        for line_number, line in enumerate(source, start=1):
            raw = line.rstrip("\n")
            if not raw.strip():
                continue
            try:
                reading = json.loads(raw)
            except json.JSONDecodeError as error:
                rejected.append({"line": line_number, "error": f"json inválido: {error}", "raw": raw})
                continue
            if not isinstance(reading, dict):
                rejected.append({"line": line_number, "error": "a linha não é um objeto JSON", "raw": raw})
                continue

            version = reading.get("schema_version")
            validator = validators.get(version)
            if validator is None:
                supported = ", ".join(str(v) for v in sorted(validators))
                rejected.append({
                    "line": line_number,
                    "error": f"schema_version {version!r} não suportado (conhecidos: {supported})",
                    "raw": raw,
                })
                continue

            errors = sorted(validator.iter_errors(reading), key=str)
            if errors:
                rejected.append({"line": line_number, "error": errors[0].message, "raw": raw})
                continue
            try:
                utc_event_date(reading)
            except ValueError as error:
                rejected.append({"line": line_number, "error": f"event_time: {error}", "raw": raw})
                continue
            valid.append(reading)
    return valid, rejected


def group_by_event_date(readings: list[dict]) -> dict[str, list[dict]]:
    partitions: dict[str, list[dict]] = defaultdict(list)
    for reading in readings:
        partitions[utc_event_date(reading)].append(reading)
    return partitions


def to_table(readings: list[dict], source_file: str, batch_id: str, ingested_at: datetime) -> pa.Table:
    rows = []
    for reading in readings:
        rows.append({
            "schema_version": reading["schema_version"],
            "reading_id": reading["reading_id"],
            "line_id": reading["line_id"],
            "machine_id": reading["machine_id"],
            "sensor_id": reading["sensor_id"],
            "sensor_type": reading["sensor_type"],
            "value": float(reading["value"]),
            "unit": reading["unit"],
            "event_time": datetime.fromisoformat(reading["event_time"]).astimezone(timezone.utc),
            "injected_anomaly": reading.get("injected_anomaly"),
            "anomaly_type": reading.get("anomaly_type"),
            "ingested_at": ingested_at,
            "source_file": source_file,
            "batch_id": batch_id,
        })
    return pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)


def dedup_by_reading_id(table: pa.Table) -> pa.Table:
    """Mantém a PRIMEIRA ocorrência de cada reading_id.

    "Primeira" = a que já estava gravada, quando a tabela existente é
    concatenada antes do lote novo. Assim um reprocessamento não altera os
    metadados de linhagem do dado que já estava na Bronze.
    """
    seen: set[str] = set()
    keep: list[bool] = []
    for reading_id in table.column("reading_id").to_pylist():
        is_new = reading_id not in seen
        seen.add(reading_id)
        keep.append(is_new)
    return table.filter(pa.array(keep))


def partition_key(event_date: str) -> str:
    return f"{BRONZE_PREFIX}/dt={event_date}/part-0000.parquet"


def read_existing_partition(client, bucket: str, event_date: str) -> pa.Table | None:
    key = partition_key(event_date)
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except client.exceptions.NoSuchKey:
        return None
    except client.exceptions.ClientError as error:  # pragma: no cover - defensivo
        if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
            return None
        raise
    return pq.read_table(io.BytesIO(body))


def write_partition(client, bucket: str, event_date: str, readings: list[dict],
                    source_file: str, batch_id: str, ingested_at: datetime) -> dict:
    """Grava uma partição de data na Bronze, em Parquet, de forma idempotente.

    Ler → mesclar → deduplicar por reading_id → sobrescrever a partição.
    Reexecutar a mesma entrada é um no-op em conteúdo; ingerir outro arquivo do
    mesmo dia acrescenta apenas as leituras novas.
    """
    incoming = dedup_by_reading_id(to_table(readings, source_file, batch_id, ingested_at))
    existing = read_existing_partition(client, bucket, event_date)

    if existing is None:
        merged = incoming
    else:
        merged = dedup_by_reading_id(
            pa.concat_tables([existing.cast(BRONZE_SCHEMA), incoming])
        )

    merged = merged.sort_by([("event_time", "ascending"), ("reading_id", "ascending")])

    buffer = io.BytesIO()
    pq.write_table(merged, buffer, compression="snappy")
    client.put_object(Bucket=bucket, Key=partition_key(event_date), Body=buffer.getvalue())

    before = 0 if existing is None else existing.num_rows
    return {"total": merged.num_rows, "novos": merged.num_rows - before, "recebidos": len(readings)}


def write_quarantine(client, bucket: str, rejected: list[dict], source_file: str,
                     batch_id: str, ingested_at: datetime) -> str:
    """Grava os rejeitados em JSON Lines, com chave determinística pelo lote."""
    # A chave depende só do lote: reprocessar o mesmo arquivo em qualquer dia
    # reescreve o mesmo objeto, em vez de criar uma cópia por data de ingestão.
    key = f"{QUARANTINE_PREFIX}/batch={batch_id}.jsonl"
    lines = [
        json.dumps({
            "source_file": source_file,
            "batch_id": batch_id,
            "line": item["line"],
            "error": item["error"],
            "raw": item["raw"],
            "quarantined_at": ingested_at.isoformat(),
        }, ensure_ascii=False)
        for item in rejected
    ]
    client.put_object(Bucket=bucket, Key=key, Body=("\n".join(lines) + "\n").encode())
    return key


def ingest_file(client, bucket: str, path: Path, validators) -> None:
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
    batch_id = batch_id_of(path)
    source_file = path.name
    readings, rejected = read_readings(path, validators)

    print(f"\n{source_file} (batch {batch_id}): "
          f"{len(readings)} válidas, {len(rejected)} rejeitadas")

    for event_date, partition in sorted(group_by_event_date(readings).items()):
        stats = write_partition(client, bucket, event_date, partition,
                                source_file, batch_id, ingested_at)
        print(f"  dt={event_date}: recebidas {stats['recebidos']:>6}  "
              f"novas {stats['novos']:>6}  total na partição {stats['total']:>6}")

    if rejected:
        key = write_quarantine(client, bucket, rejected, source_file, batch_id, ingested_at)
        motivos: dict[str, int] = {}
        for item in rejected:
            motivos[item["error"][:60]] = motivos.get(item["error"][:60], 0) + 1
        print(f"  quarentena: {len(rejected)} registros em s3://{bucket}/{key}")
        for motivo, quantidade in sorted(motivos.items(), key=lambda kv: -kv[1])[:5]:
            print(f"    {quantidade:>5}x {motivo}")


def main(argv: list[str]) -> None:
    if not argv:
        print(__doc__.splitlines()[2].strip(), file=sys.stderr)
        raise SystemExit(2)
    load_dotenv()
    validators = load_validators()
    client = s3_client()
    bucket = os.environ["S3_BUCKET"]
    ensure_bucket(client, bucket)
    for argument in argv:
        ingest_file(client, bucket, Path(argument), validators)


if __name__ == "__main__":
    main(sys.argv[1:])
