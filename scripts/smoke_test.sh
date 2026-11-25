#!/usr/bin/env bash
# Teste ponta a ponta da Entrega 1 contra a plataforma no ar.
#
#   ./scripts/smoke_test.sh
#
# Verifica, nesta ordem:
#   1. catálogo populado e consultas respondendo;
#   2. ingestão grava Parquet particionado por data;
#   3. REEXECUTAR a mesma ingestão não duplica nada;
#   4. ingerir OUTRO arquivo do mesmo dia soma, sem perder o que já havia;
#   5. registros inválidos vão para a quarentena, não para a Bronze.
set -euo pipefail
cd "$(dirname "$0")/.."

PY=${PY:-.venv/bin/python}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

verde() { printf '\033[32m  ok\033[0m %s\n' "$1"; }
falha() { printf '\033[31m  FALHOU\033[0m %s\n' "$1"; exit 1; }

contar_bronze() {
  $PY - <<'PY'
import io, os
import boto3, pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(".env")  # caminho explícito: o script chega por stdin
client = boto3.client(
    "s3", endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"], region_name="us-east-1",
    config=Config(request_checksum_calculation="when_required",
                  response_checksum_validation="when_required",
                  s3={"addressing_style": "path"}))
bucket = os.environ["S3_BUCKET"]
paginas = client.get_paginator("list_objects_v2")
linhas = objetos = 0
ids = set()
for pagina in paginas.paginate(Bucket=bucket, Prefix="bronze/sensor_readings/"):
    for item in pagina.get("Contents", []):
        objetos += 1
        corpo = client.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
        tabela = pq.read_table(io.BytesIO(corpo))
        linhas += tabela.num_rows
        ids.update(tabela.column("reading_id").to_pylist())
quarentena = sum(len(p.get("Contents", []))
                 for p in paginas.paginate(Bucket=bucket, Prefix="quarantine/"))
print(objetos, linhas, len(ids), quarentena)
PY
}

echo "== 1. catálogo NoSQL"
$PY catalog/seed_catalog.py --machines 6 --lines 2 --interval 60 >/dev/null
$PY catalog/example_queries.py | grep -q "IXSCAN" \
  && verde "consultas do catálogo usam índice (IXSCAN)" \
  || falha "as consultas do catálogo não usaram índice"

echo "== 2. primeira ingestão"
$PY simulator/simulator.py --machines 6 --lines 2 --count 4000 --interval 60 \
    --seed 42 --start 2026-03-01T22:00:00 --anomaly-rate 0.002 --invalid-rate 0.01 \
    > "$WORK/lote_a.jsonl"
$PY ingest/ingest_bronze.py "$WORK/lote_a.jsonl" >/dev/null
read -r OBJ1 LIN1 IDS1 QUAR1 <<<"$(contar_bronze)"
[ "$OBJ1" -ge 2 ] && verde "Bronze particionada: $OBJ1 objetos, $LIN1 linhas" \
  || falha "esperava ao menos 2 partições (o lote cruza a meia-noite UTC)"
[ "$LIN1" -eq "$IDS1" ] && verde "nenhum reading_id duplicado" \
  || falha "há reading_id duplicado: $LIN1 linhas para $IDS1 ids"
[ "$QUAR1" -ge 1 ] && verde "quarentena criada ($QUAR1 objeto)" \
  || falha "os registros inválidos não foram para a quarentena"

echo "== 3. reexecução da MESMA entrada (idempotência)"
$PY ingest/ingest_bronze.py "$WORK/lote_a.jsonl" >/dev/null
read -r OBJ2 LIN2 IDS2 _ <<<"$(contar_bronze)"
[ "$LIN2" -eq "$LIN1" ] && [ "$OBJ2" -eq "$OBJ1" ] \
  && verde "reexecutar não duplicou nada ($LIN2 linhas, $OBJ2 objetos)" \
  || falha "a reexecução mudou a Bronze: $LIN1 -> $LIN2 linhas"

echo "== 4. segundo lote do MESMO dia (não pode apagar o anterior)"
$PY simulator/simulator.py --machines 6 --lines 2 --count 2000 --interval 60 \
    --seed "$(date +%s)" --start 2026-03-01T22:30:00 --anomaly-rate 0.002 \
    > "$WORK/lote_b.jsonl"
$PY ingest/ingest_bronze.py "$WORK/lote_b.jsonl" >/dev/null
read -r _ LIN3 IDS3 _ <<<"$(contar_bronze)"
[ "$LIN3" -gt "$LIN2" ] && [ "$LIN3" -eq "$IDS3" ] \
  && verde "lote novo somado sem perda nem duplicata ($LIN2 -> $LIN3 linhas)" \
  || falha "o segundo lote sobrescreveu ou duplicou dados ($LIN2 -> $LIN3)"

echo
printf '\033[32mSmoke test completo: a Entrega 1 está funcional.\033[0m\n'
