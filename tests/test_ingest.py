"""Testes da ingestão Bronze — funções puras, sem Docker nem rede.

A idempotência ponta a ponta contra o object storage real é verificada em
scripts/smoke_test.sh.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingest.ingest_bronze import (  # noqa: E402
    BRONZE_SCHEMA,
    batch_id_of,
    dedup_by_reading_id,
    group_by_event_date,
    load_validators,
    partition_key,
    read_readings,
    to_table,
    utc_event_date,
)

AGORA = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)


def leitura(reading_id="r-1", event_time="2026-02-10T23:30:00.000Z", **extra) -> dict:
    base = {
        "schema_version": 2,
        "reading_id": reading_id,
        "line_id": "L-01",
        "machine_id": "M-001",
        "sensor_id": "M-001-TMP",
        "sensor_type": "temperature",
        "value": 65.0,
        "unit": "celsius",
        "event_time": event_time,
        "injected_anomaly": False,
    }
    base.update(extra)
    return base


@pytest.fixture(scope="module")
def validators():
    return load_validators()


def escreve(tmp_path: Path, linhas: list[str]) -> Path:
    destino = tmp_path / "readings.jsonl"
    destino.write_text("\n".join(linhas) + "\n")
    return destino


def test_contratos_v1_e_v2_sao_carregados(validators):
    assert set(validators) == {1, 2}


def test_validacao_por_versao_do_contrato(tmp_path, validators):
    v1 = leitura("r-v1")
    v1["schema_version"] = 1
    v1.pop("anomaly_type", None)
    v2 = leitura("r-v2", injected_anomaly=True, anomaly_type="spike")
    futura = leitura("r-v9")
    futura["schema_version"] = 9

    validas, rejeitadas = read_readings(
        escreve(tmp_path, [json.dumps(v1), json.dumps(v2), json.dumps(futura)]), validators)

    assert [r["reading_id"] for r in validas] == ["r-v1", "r-v2"]
    assert len(rejeitadas) == 1
    assert "não suportado" in rejeitadas[0]["error"]


def test_anomaly_type_e_rejeitado_na_v1(tmp_path, validators):
    """anomaly_type só existe na v2: a v1 tem additionalProperties: false."""
    v1 = leitura("r-x", injected_anomaly=True, anomaly_type="spike")
    v1["schema_version"] = 1
    _, rejeitadas = read_readings(escreve(tmp_path, [json.dumps(v1)]), validators)
    assert len(rejeitadas) == 1


@pytest.mark.parametrize("linha,trecho", [
    ('{"schema_version": 2, "reading_id": "truncado', "json inválido"),
    ("[1, 2, 3]", "não é um objeto"),
])
def test_linhas_malformadas_vao_para_a_quarentena(tmp_path, validators, linha, trecho):
    validas, rejeitadas = read_readings(escreve(tmp_path, [linha]), validators)
    assert validas == []
    assert trecho in rejeitadas[0]["error"]
    assert rejeitadas[0]["raw"] == linha  # a linha original é preservada


def test_timestamp_sem_fuso_e_rejeitado(tmp_path, validators):
    ruim = leitura("r-naive", event_time="2026-02-10T23:30:00.000")
    validas, rejeitadas = read_readings(escreve(tmp_path, [json.dumps(ruim)]), validators)
    assert validas == []
    assert rejeitadas


def test_particao_usa_a_data_utc_do_evento():
    # 23:30-03:00 é 02:30 UTC do dia seguinte: a partição segue o UTC.
    assert utc_event_date(leitura(event_time="2026-02-10T23:30:00-03:00")) == "2026-02-11"
    assert utc_event_date(leitura(event_time="2026-02-10T23:30:00.000Z")) == "2026-02-10"


def test_agrupamento_separa_os_dias():
    grupos = group_by_event_date([
        leitura("a", "2026-02-10T10:00:00.000Z"),
        leitura("b", "2026-02-10T20:00:00.000Z"),
        leitura("c", "2026-02-11T01:00:00.000Z"),
    ])
    assert {dia: len(v) for dia, v in grupos.items()} == {"2026-02-10": 2, "2026-02-11": 1}


def test_layout_da_particao_e_hive():
    assert partition_key("2026-02-10") == "bronze/sensor_readings/dt=2026-02-10/part-0000.parquet"


def test_dedup_mantem_a_primeira_ocorrencia():
    antiga = to_table([leitura("r-1", value=1.0)], "velho.jsonl", "aaa", AGORA)
    nova = to_table([leitura("r-1", value=2.0), leitura("r-2")], "novo.jsonl", "bbb", AGORA)
    juntas = dedup_by_reading_id(pa.concat_tables([antiga, nova]))
    assert juntas.num_rows == 2
    linha = juntas.filter(pc.equal(juntas.column("reading_id"), "r-1")).to_pylist()[0]
    assert linha["batch_id"] == "aaa"      # a linhagem original é preservada
    assert linha["source_file"] == "velho.jsonl"


def test_tabela_respeita_o_esquema_explicito():
    tabela = to_table([leitura(injected_anomaly=True, anomaly_type="drift")],
                      "x.jsonl", "abc", AGORA)
    assert tabela.schema == BRONZE_SCHEMA
    linha = tabela.to_pylist()[0]
    assert linha["event_time"].tzinfo is not None
    assert linha["anomaly_type"] == "drift"
    assert linha["batch_id"] == "abc"


def test_anomaly_type_ausente_vira_nulo():
    assert to_table([leitura()], "x.jsonl", "abc", AGORA).to_pylist()[0]["anomaly_type"] is None


def test_batch_id_e_deterministico_pelo_conteudo(tmp_path):
    original = escreve(tmp_path, [json.dumps(leitura("r-1"))])
    copia = tmp_path / "copia.jsonl"
    copia.write_text(original.read_text())
    diferente = tmp_path / "diferente.jsonl"
    diferente.write_text(json.dumps(leitura("r-2")) + "\n")

    assert batch_id_of(original) == batch_id_of(copia)
    assert batch_id_of(original) != batch_id_of(diferente)
