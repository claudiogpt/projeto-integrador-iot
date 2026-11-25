# Modelagem NoSQL — catálogo de máquinas e sensores

Documento que acompanha `catalog/seed_catalog.py` e `catalog/example_queries.py`.
A decisão de tipo de banco está em [ADR-0002](adr/0002-nosql-mongodb-documento.md).

## 1. Padrões de acesso (o que veio primeiro)

O modelo foi desenhado a partir das consultas, não o contrário. Estes são os
acessos que a plataforma faz ao catálogo:

| # | Padrão de acesso | Quem usa | Frequência |
|---|---|---|---|
| P1 | dado um `sensor_id`, obter máquina, linha e limiares | enriquecimento Bronze → Silver; detector de anomalias | altíssima (1× por leitura) |
| P2 | máquinas ativas de uma linha | agregações por linha (Gold) | média |
| P3 | tabela de limiares de todos os sensores ativos | detector de anomalias (Entrega 3), carregada na subida | baixa |
| P4 | inventário por linha e tipo de sensor | dashboard (Entrega 2) | baixa |
| P5 | cadastrar/alterar máquina ou sensor | operação | rara |

**P1 domina o desenho.** É ele que justifica embutir os sensores: resolver o
acesso mais frequente com uma única leitura e nenhum join.

## 2. Modelo

Duas coleções. `machines` concentra o modelo; `lines` existe para os atributos
próprios da linha.

```javascript
// machines — um documento por máquina
{
  "_id": "M-001",                       // o próprio machine_id é a chave
  "line_id": "L-01",
  "line": { "line_id": "L-01", "name": "Linha 01" },   // referência estendida
  "model": "ACM-800",
  "status": "active",                   // active | maintenance | retired
  "installed_at": ISODate("2024-01-01T00:00:00Z"),
  "sensors": [                          // embutidos: 4 por máquina, lidos juntos
    {
      "sensor_id": "M-001-TMP",
      "sensor_type": "temperature",
      "unit": "celsius",
      "sampling_interval_s": 60.0,
      "normal_range":    { "min": 56.0, "max": 74.0 },   // média ± 3σ
      "alert_threshold": { "min": 51.5, "max": 78.5 },   // média ± 4.5σ
      "status": "active"
    }
    // ... VIB, PRS, PWR
  ]
}

// lines — atributos da linha, sem duplicar o que já está em machines.line
{ "_id": "L-01", "name": "Linha 01", "plant": "AcmeTech BH" }
```

### Por que cada escolha

| Escolha | Padrão de modelagem | Motivo |
|---|---|---|
| `_id` = `machine_id` | chave natural | elimina um índice e garante unicidade de graça |
| sensores embutidos | *embedded one-to-few* | 4 por máquina, limite conhecido, sempre lidos no contexto da máquina |
| `line` dentro da máquina | *extended reference* | evita um lookup só para exibir o nome da linha; o resto fica em `lines` |
| limiares no documento | configuração como dado | o detector da Entrega 3 lê do catálogo, não do código |
| `status` em máquina e sensor | *soft delete* | preserva o histórico das leituras de um sensor removido |

### Por que não desnormalizar mais

Uma coleção `sensors` separada, com `machine_id` como referência, faria P1 em
uma leitura também — mas custaria um `$lookup` para trazer linha e modelo, que é
exatamente o que P1 precisa. Embutir resolve os dois de uma vez.

## 3. Índices

| Índice | Tipo | Atende | Observação |
|---|---|---|---|
| `_id` | padrão | P5 | chave natural |
| `ix_sensor_id` em `sensors.sensor_id` | multikey, **único** | P1 | o único garante que um `sensor_id` não apareça em duas máquinas |
| `ix_line_status` em `(line_id, status)` | composto | P2 | ordem: igualdade antes de filtro |
| `ix_sensor_type` em `sensors.sensor_type` | multikey | P3, P4 | |

Verificação real, por `catalog/example_queries.py`:

```
P1 — enriquecimento de leitura (M-001-TMP)
    plano: PROJECTION_DEFAULT → FETCH → IXSCAN
P2 — máquinas ativas da linha L-01
    plano: FETCH → IXSCAN
```

Nenhum `COLLSCAN` nas consultas críticas.

## 4. Integridade

A coleção `machines` tem validador `$jsonSchema` com
`validationAction: "error"`, aplicado via `collMod` (idempotente). Ele exige o
padrão `^M-[0-9]{3}$` no `_id`, `^M-[0-9]{3}-(TMP|VIB|PRS|PWR)$` em cada
`sensor_id`, e restringe `sensor_type`, `unit` e `status` a enums.

A consulta P5 do script de exemplo tenta inserir `_id: "MAQUINA-X"` e mostra o
`WriteError` — a evidência de que o validador está ativo.

## 5. Coerência com o simulador

`seed_catalog.py` importa `build_fleet` e `sensor_id_of` **do próprio
simulador**. Não há duas listas de máquinas para manter em sincronia: passando
os mesmos `--machines/--lines` aos dois, todo `sensor_id` gerado existe no
catálogo. As faixas também derivam dos mesmos `SENSOR_PROFILES`, o que torna o
gabarito do simulador comparável aos limiares do catálogo na Entrega 3.

## 6. O que muda nas próximas entregas

- **Entrega 2:** o job Bronze → Silver lê o catálogo para enriquecer as leituras
  (P1) e validar unidade contra `sensors.unit`.
- **Entrega 3:** os `alert_threshold` alimentam o detector; a tabela de limiares
  (P3) provavelmente vai para o Redis como cache no caminho de streaming.
