# ADR-0002 — MongoDB (documento) para o catálogo

**Status:** aceito · **Entrega:** 1

## Contexto
O catálogo guarda máquinas, seus sensores, as faixas normais de operação e os
limiares de alerta. O padrão de acesso dominante, que ocorre uma vez por
leitura ingerida, é: **dado um `sensor_id`, obter a máquina, a linha e os
limiares daquele sensor**.

## Decisão
**Banco de documento (MongoDB 7.0)**, um documento por máquina, com os sensores
**embutidos** em um array e a linha como **referência estendida**
(`line_id` + nome).

## Justificativa
- A unidade de acesso é a máquina inteira: 1 leitura resolve o padrão P1, sem join.
- Cardinalidade dos sensores é baixa e limitada (4 por máquina) e eles nunca são
  consultados fora do contexto da máquina — caso clássico de *embedding*.
- O documento é pequeno (~2 KB), muito longe do limite de 16 MB do BSON.
- Sensores de tipos diferentes podem ter atributos distintos no futuro
  (calibração, fabricante): o esquema flexível absorve isso sem migração.

## Alternativas consideradas
| Tipo | Por que não |
|---|---|
| Chave-valor (Redis) | resolveria P1 (`GET sensor:M-001-TMP`), mas não as consultas por linha, por tipo e as agregações do dashboard, sem manter vários índices à mão |
| Colunar (Cassandra) | exigiria uma tabela por padrão de consulta e desnormalização manual; o catálogo é pequeno e muda pouco, então o ganho de escrita não compensa |
| Grafo (Neo4j) | as relações são uma hierarquia fixa de dois níveis (linha → máquina → sensor), sem travessias profundas que justifiquem um grafo |

Redis volta a ser candidato na Entrega 3, como **cache** dos limiares no
caminho de streaming — o que é um papel diferente do catálogo.

## Consequências
- Índice multikey único em `sensors.sensor_id` resolve P1 com IXSCAN.
- Validador `$jsonSchema` em `machines` impede documento fora do padrão.
- Alterar um sensor exige reescrever o documento da máquina — aceitável, já que
  o catálogo é lido com altíssima frequência e escrito raramente.
