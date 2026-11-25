# AcmeTech — Plataforma de Dados para IoT Industrial

Projeto Integrador de **Integração de Dados II** · PUC Minas · 1º semestre de 2026
Prof. Ricardo Carlini Sperandio

> **Entrega 1 — Fundação.** Infraestrutura como código, simulador de sensores,
> modelagem NoSQL do catálogo e ingestão idempotente na camada Bronze.

## Grupo

| Nome          | Matrícula   | Responsabilidade principal nesta entrega |
|Cláudio Augusto| 835748      | TUDO                                     |

---

## 1. O que já funciona nesta entrega

```
                            ┌──────────────── Entrega 1 ────────────────┐

  ┌───────────┐  JSON Lines  ┌───────────┐   Parquet     ┌──────────────────────┐
  │ Simulador │─────────────▶│ Ingestão  │──────────────▶│  Object storage (S3) │
  │ de        │              │  Bronze   │  dt=YYYY-MM-DD│  bronze/…            │
  │ sensores  │              │           │──────────────▶│  quarantine/…        │
  └───────────┘              └─────┬─────┘  rejeitados   └──────────────────────┘
                                   │
                                   │ valida contrato (v1/v2)
                                   ▼
                            ┌─────────────┐
                            │  MongoDB    │  catálogo de máquinas/sensores
                            │  (catálogo) │  + faixas e limiares
                            └─────────────┘

                 Entrega 2: Spark + Airflow → Silver/Gold + dashboard
                 Entrega 3: Kafka → streaming, alertas, API externa
```

Nesta entrega o simulador grava **direto** na Bronze, como prevê o enunciado; o
Kafka entra só na Entrega 3.

## 2. Pré-requisitos

- Docker com Compose v2 (`docker compose version`)
- Python 3.12
- Portas livres: **8333** (API S3), **9333** (status), **27017** (MongoDB)

## 3. Executando do zero

```sh
git clone <URL-DO-REPOSITORIO> && cd projeto-integrador
```

```sh
cp .env.example .env
```

Edite o `.env` e troque as senhas. Ele **nunca** é versionado (está no
`.gitignore`); o `.env.example` é o único arquivo de credenciais no Git, e com
valores de exemplo. A `MONGO_URI` referencia `${MONGO_USER}`/`${MONGO_PASSWORD}`,
então basta trocar a senha em um lugar.

> O MongoDB só aplica usuário e senha na **primeira** criação do volume. Se você
> trocar as credenciais depois de já ter subido a plataforma, rode
> `docker compose down -v` antes de subir de novo.

```sh
docker compose up -d --wait
```

Isso sobe o object storage, o MongoDB e cria o bucket do lakehouse. O `--wait`
só retorna quando os healthchecks passam. Se faltar alguma variável no `.env`, o
Compose falha na hora dizendo qual é.

```sh
python3.12 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
```

```sh
python catalog/seed_catalog.py --machines 6 --lines 2 --interval 60
```

```sh
mkdir -p data && python simulator/simulator.py --machines 6 --lines 2 --count 5000 --interval 60 --seed 42 --start 2026-03-01T22:00:00 --anomaly-rate 0.002 --invalid-rate 0.01 --stats > data/readings.jsonl
```

```sh
python ingest/ingest_bronze.py data/readings.jsonl
```

Saída real deste comando (o lote começa às 22:00 UTC e cruza a meia-noite, por
isso duas partições):

```
readings.jsonl (batch cc40df689e53bdf5): 4944 válidas, 56 rejeitadas
  dt=2026-03-01: recebidas   2848  novas   2848  total na partição   2848
  dt=2026-03-02: recebidas   2096  novas   2096  total na partição   2096
  quarentena: 56 registros em s3://lakehouse/quarantine/sensor_readings/batch=cc40df689e53bdf5.jsonl
       10x Additional properties are not allowed ('operator_notes' was
        9x 'unit' is a required property
        8x 'humidity' is not one of ['temperature', 'vibration', 'press
```

Rode o **mesmo comando de ingestão outra vez**: a coluna `novas` zera e o total
não muda. É a idempotência exigida pelo barema.

```
  dt=2026-03-01: recebidas   2848  novas      0  total na partição   2848
  dt=2026-03-02: recebidas   2096  novas      0  total na partição   2096
```

Para encerrar: `docker compose down` (preserva os dados) ou
`docker compose down -v` (apaga lakehouse e catálogo).

> Há um `Makefile` com atalhos para todos esses comandos (`make help`). Ele é
> conveniência: nada depende dele.

## 4. Verificando a entrega

```sh
.venv/bin/python -m pytest -q tests
```

22 testes unitários do simulador e da ingestão; não precisam de Docker.

```sh
./scripts/smoke_test.sh
```

Teste ponta a ponta contra a plataforma no ar. Ele verifica, com a saída real:

| # | O que verifica |
|---|---|
| 1 | catálogo populado e consultas usando índice (IXSCAN, não COLLSCAN) |
| 2 | Bronze em Parquet, particionada por data, sem `reading_id` duplicado |
| 3 | reexecutar a mesma entrada **não duplica** nada |
| 4 | um segundo lote do mesmo dia **soma** sem apagar o anterior |
| 5 | registros inválidos vão para a quarentena, nunca para a Bronze |

```sh
.venv/bin/python catalog/example_queries.py
```

Uma consulta por padrão de acesso, com o plano de execução de cada uma.

## 5. Estrutura do repositório

```
projeto-integrador/
├── docker-compose.yml          infraestrutura como código (Entrega 1)
├── .env.example                modelo de credenciais; o .env fica fora do Git
├── Makefile                    atalhos para os comandos deste README
├── contracts/
│   ├── sensor_reading.v1.schema.json   contrato original do starter kit
│   └── sensor_reading.v2.schema.json   v2: acrescenta anomaly_type (gabarito)
├── simulator/simulator.py      gerador de leituras sintéticas
├── ingest/ingest_bronze.py     validação, quarentena e escrita Parquet idempotente
├── catalog/
│   ├── seed_catalog.py         cria coleções, validador, índices e popula
│   └── example_queries.py      uma consulta por padrão de acesso, com explain()
├── tests/                      testes unitários (sem Docker)
├── scripts/smoke_test.sh       teste ponta a ponta
└── docs/
    ├── modelagem_nosql.md      padrões de acesso, modelo, índices, justificativa
    └── adr/                    decisões de arquitetura (ADR-0001 a 0004)
```

## 6. Simulador

```sh
python simulator/simulator.py --help
```

| Opção | Padrão | Efeito |
|---|---|---|
| `--machines` / `--lines` | 6 / 2 | tamanho do parque (`M-001…`, `L-01…`) |
| `--interval` | 1.0 | segundos entre duas leituras do mesmo sensor |
| `--count` | infinito | para após N leituras |
| `--anomaly-rate` | 0.01 | probabilidade de **iniciar** um episódio de anomalia |
| `--invalid-rate` | 0.0 | fração de linhas que **violam** o contrato |
| `--seed` | aleatório | fixa os valores gerados |
| `--start` | agora | primeiro `event_time` (ISO 8601; sem fuso = UTC) |
| `--realtime` | desligado | dorme entre as rodadas (produtor Kafka na Entrega 3) |
| `--stats` | desligado | resumo por tipo de anomalia em stderr |

### Anomalias ≠ registros inválidos

Esta distinção é deliberada, e cada lado exercita um componente diferente:

- **Anomalia** (`--anomaly-rate`): leitura **válida** no contrato, com valor
  anormal. É o alvo do **detector de anomalias** (Entrega 3). Vem marcada com
  `injected_anomaly: true` e `anomaly_type`: o **gabarito**, usado para medir o
  detector, nunca para detectar.
- **Registro inválido** (`--invalid-rate`): linha que **viola** o contrato. É o
  alvo da **validação de qualidade**: a ingestão rejeita e manda para a
  quarentena.

### Tipos de anomalia

Anomalias são **episódios com estado**, não eventos isolados — uma deriva ou um
sensor travado duram várias leituras do mesmo sensor:

| Tipo | Duração | Comportamento |
|---|---|---|
| `spike` | 1–3 leituras | pico abrupto de 4σ a 9σ (ex.: superaquecimento momentâneo) |
| `drift` | 20–60 leituras | rampa linear: o desvio cresce progressivamente até 4σ–9σ |
| `stuck` | 15–40 leituras | sensor travado: repete **exatamente** o mesmo valor |

Por isso `--anomaly-rate` é a probabilidade de **iniciar** um episódio, não a
fração de leituras anômalas. Vale aproximadamente:

```
fração_anômala ≈ (taxa × duração_média) / (1 + taxa × duração_média)
```

Com duração média de ~23 leituras, `--anomaly-rate 0.002` dá ~4% de leituras
anômalas e `0.02` dá ~30%. O `--stats` imprime a fração real de cada execução.

### Tipos de registro inválido

`missing_required` (sem `unit`), `bad_enum` (`sensor_type: "humidity"`),
`bad_pattern` (`machine_id: "MAQ-1"`), `wrong_type` (`value` como string),
`naive_timestamp` (sem fuso), `unknown_field` (viola
`additionalProperties: false`) e `broken_json` (linha truncada).

## 7. Camada Bronze

**Layout** (Hive-style, o que o Spark da Entrega 2 lê sem configuração extra):

```
s3://lakehouse/
├── bronze/sensor_readings/dt=2026-03-01/part-0000.parquet
├── bronze/sensor_readings/dt=2026-03-02/part-0000.parquet
└── quarantine/sensor_readings/batch=cc40df689e53bdf5.jsonl
```

A partição usa a **data UTC do `event_time`**, não a data de ingestão: uma
leitura das 23:30 em UTC−3 cai na partição do dia seguinte, e um reprocessamento
sempre recai na mesma partição.

**Esquema** explícito (sem inferência de tipos), com os campos do contrato mais
três de linhagem: `ingested_at`, `source_file` e `batch_id` — o `batch_id` é o
hash SHA-256 do arquivo de entrada, então o mesmo arquivo sempre gera o mesmo id.

**Idempotência** por ler → mesclar → deduplicar por `reading_id` → sobrescrever.
Sobrescrever a partição sem mesclar seria idempotente para o mesmo arquivo, mas
apagaria os lotes anteriores do mesmo dia; o raciocínio completo está no
[ADR-0003](docs/adr/0003-idempotencia-bronze.md).

**Quarentena**: todo registro rejeitado é gravado com a linha original, o número
da linha, o motivo e o lote de origem. A chave depende só do lote
(`quarantine/sensor_readings/batch=<hash do arquivo>.jsonl`), então reprocessar
o mesmo arquivo reescreve o mesmo objeto, em qualquer dia.

## 8. Catálogo NoSQL

Resumo; o detalhamento está em [docs/modelagem_nosql.md](docs/modelagem_nosql.md)
e em [ADR-0002](docs/adr/0002-nosql-mongodb-documento.md).

**MongoDB, um documento por máquina, com os sensores embutidos.** O padrão de
acesso dominante — dado um `sensor_id`, obter máquina, linha e limiares — ocorre
uma vez por leitura ingerida e é resolvido com uma única leitura indexada, sem
join. Chave-valor não atenderia as consultas por linha e por tipo; colunar
exigiria uma tabela por consulta num catálogo que é pequeno e muda pouco; grafo
não se justifica numa hierarquia fixa de dois níveis.

As faixas normais e os limiares de alerta ficam **no catálogo**, não no código:
são eles que alimentarão o detector de anomalias da Entrega 3.

## 9. Decisões de arquitetura

| ADR | Decisão |
|---|---|
| [0001](docs/adr/0001-object-storage-seaweedfs.md) | SeaweedFS como object storage S3 (MinIO foi arquivado) |
| [0002](docs/adr/0002-nosql-mongodb-documento.md) | MongoDB (documento) para o catálogo, sensores embutidos |
| [0003](docs/adr/0003-idempotencia-bronze.md) | Idempotência por ler-mesclar-sobrescrever |
| [0004](docs/adr/0004-versionamento-do-contrato.md) | Contrato versionado: v1 e v2 validadas lado a lado |

## 10. O que mudamos em relação ao starter kit

| Área | Starter kit | Aqui |
|---|---|---|
| Credenciais | `config/s3.json` com o segredo **versionado** | renderizado em runtime a partir do `.env`; nenhum segredo no Git |
| Variáveis | ausência silenciosa | `${VAR:?mensagem}` — falha na hora, dizendo o que falta |
| Bucket | criado pelo código da ingestão | serviço de bootstrap no Compose, com `depends_on: service_healthy` |
| Healthcheck do Mongo | `ping` anônimo | autenticado, provando que o usuário do `.env` existe |
| `inject_anomaly` | `TODO`, devolvia a leitura intacta | três tipos de episódio com estado, mais o gabarito `anomaly_type` |
| Registros inválidos | inexistentes | `--invalid-rate` com sete tipos de violação |
| `write_partition` | `TODO`, levantava `NotImplementedError` | Parquet com esquema explícito, mesclagem e dedupe |
| Rejeitados | impressos em stderr e perdidos | gravados na quarentena, com motivo e linhagem |
| Contrato | v1 fixa | v1 e v2, validadas por `schema_version` |
| Catálogo | inexistente | coleções, validador `$jsonSchema`, índices e consultas |
| Testes | inexistentes | 22 testes unitários + smoke test ponta a ponta |

## 11. Limitações conhecidas

- A mesclagem reescreve a partição inteira a cada lote. Funciona no volume da
  disciplina; na Entrega 2 a alternativa é um formato de tabela com *upsert*
  nativo (Delta/Iceberg/Hudi).
- Toda a ingestão é single-node e em memória: um lote precisa caber na RAM.
  O Spark da Entrega 2 remove essa restrição.
- Sem autenticação ou TLS entre os serviços: ambiente de desenvolvimento local.
- O simulador escreve em stdout; a produção para o Kafka chega na Entrega 3
  (o modo `--realtime` já existe pensando nisso).

## 12. Uso de ferramentas de IA

_Preencher antes de submeter, conforme exigido pelo enunciado (seção 9):
declarar onde houve apoio de IA. Todo o código entregue é compreendido pelo
grupo e defensável na demonstração._

## 13. Submissão (checklist do roteiro)

- [ ] `.env` fora do Git; `.env.example` versionado
- [ ] `git clone` + `docker compose up -d --wait` funciona em máquina limpa
- [ ] Simulador com volume, frequência e anomalias configuráveis
- [ ] Modelagem NoSQL justificada (`docs/modelagem_nosql.md` + ADR-0002)
- [ ] Bronze em Parquet particionada por data; reexecução não duplica
- [ ] README com o passo a passo de execução
- [ ] Nomes e matrículas do grupo preenchidos (seção 1) e uso de IA declarado (seção 12)
- [ ] Histórico de commits evidenciando a contribuição de cada integrante
- [ ] Tag `entrega-1` criada e enviada

```sh
git tag -a entrega-1 -m "Entrega 1 — Fundação" && git push origin entrega-1
```
