"""Testes do simulador — não precisam de Docker nem de rede."""

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from simulator.simulator import SENSOR_PROFILES, build_fleet, generate, parse_args  # noqa: E402

CONTRACT_V2 = Path(__file__).resolve().parent.parent / "contracts" / "sensor_reading.v2.schema.json"


@pytest.fixture(scope="module")
def validator():
    return Draft202012Validator(json.loads(CONTRACT_V2.read_text()))


def run(*args: str):
    return list(generate(parse_args(list(args))))


def test_leituras_sem_corrupcao_respeitam_o_contrato(validator):
    for payload, _ in run("--count", "500", "--seed", "1", "--anomaly-rate", "0.2"):
        assert payload is not None
        assert not list(validator.iter_errors(payload)), payload


def test_semente_e_start_tornam_a_execucao_byte_a_byte_reproduzivel():
    args = ("--count", "200", "--seed", "7", "--anomaly-rate", "0.1",
            "--start", "2026-03-01T22:00:00")
    assert run(*args) == run(*args)


def test_semente_sozinha_fixa_os_valores_mas_nao_os_timestamps():
    """Sem --start o event_time vem do relógio; os valores continuam fixos."""
    args = ("--count", "200", "--seed", "7", "--anomaly-rate", "0.1")
    primeira, segunda = run(*args), run(*args)
    campos = lambda saida: [  # noqa: E731
        {k: v for k, v in payload.items() if k != "event_time"} for payload, _ in saida
    ]
    assert campos(primeira) == campos(segunda)


def test_sem_anomalia_nenhuma_leitura_vem_marcada():
    for payload, label in run("--count", "300", "--seed", "3", "--anomaly-rate", "0"):
        assert payload["injected_anomaly"] is False
        assert "anomaly_type" not in payload
        assert label == "normal"


def test_episodios_cobrem_os_tres_tipos_de_anomalia():
    rotulos = {label for _, label in run("--count", "4000", "--seed", "11",
                                         "--anomaly-rate", "0.05")}
    assert {"spike", "drift", "stuck"} <= rotulos


def test_sensor_travado_repete_exatamente_o_mesmo_valor():
    por_sensor: dict[str, list[float]] = {}
    for payload, label in run("--count", "6000", "--seed", "5", "--anomaly-rate", "0.05"):
        if label == "stuck":
            por_sensor.setdefault(payload["sensor_id"], []).append(payload["value"])
    assert por_sensor, "nenhum episódio de sensor travado foi gerado"
    # Em um episódio travado o valor não muda; sensores com vários episódios
    # podem ter mais de um valor distinto, mas nunca um por leitura.
    for sensor_id, valores in por_sensor.items():
        assert len(set(valores)) < len(valores), sensor_id


def test_anomalias_ficam_fora_da_faixa_normal():
    fora = 0
    total = 0
    for payload, label in run("--count", "6000", "--seed", "13", "--anomaly-rate", "0.05"):
        if label != "spike":
            continue
        perfil = SENSOR_PROFILES[payload["sensor_type"]]
        total += 1
        if abs(payload["value"] - perfil.mean) > 3 * perfil.stddev:
            fora += 1
    assert total > 0
    assert fora / total > 0.9, "picos deveriam sair da faixa normal de 3 sigmas"


def test_registros_invalidos_sao_gerados_e_so_quando_pedidos(validator):
    quebrados = 0
    for payload, _ in run("--count", "800", "--seed", "2", "--invalid-rate", "0.2"):
        if payload is None or list(validator.iter_errors(payload)):
            quebrados += 1
    assert quebrados > 0

    for payload, _ in run("--count", "400", "--seed", "2", "--invalid-rate", "0"):
        assert payload is not None
        assert not list(validator.iter_errors(payload))


def test_frota_distribui_maquinas_entre_as_linhas():
    frota = build_fleet(6, 2)
    assert frota[0] == ("L-01", "M-001")
    assert frota[1] == ("L-02", "M-002")
    assert len(frota) == 6
    assert len({line for line, _ in frota}) == 2
