# ADR-0003 — Idempotência da Bronze por ler-mesclar-sobrescrever

**Status:** aceito · **Entrega:** 1

## Contexto
O enunciado pede ingestão idempotente e a dica de arquitetura sugere
"particione por data e sobrescreva a partição em vez de append".

Sobrescrever a partição inteira é idempotente para o **mesmo** arquivo de
entrada, mas tem um efeito colateral grave: ingerir um **segundo** arquivo que
contenha leituras do mesmo dia **apagaria** as leituras do primeiro. Em um
cenário de sensores, em que os lotes chegam ao longo do dia, isso é perda de dado.

## Decisão
Para cada partição de data: **ler o Parquet existente → concatenar o lote novo →
deduplicar por `reading_id`, mantendo a primeira ocorrência → sobrescrever**.

## Consequências
- Reexecutar a mesma entrada é um no-op em conteúdo (verificado no smoke test).
- Lotes diferentes do mesmo dia somam, sem duplicar nem perder.
- A linhagem (`source_file`, `batch_id`, `ingested_at`) do dado que já estava
  gravado é preservada, porque a dedupe mantém a primeira ocorrência.
- Custo: a partição inteira é lida e reescrita a cada lote. Aceitável no volume
  da disciplina; na Entrega 2 o Spark assume e a alternativa natural é um
  formato de tabela com *upsert* nativo (Delta/Iceberg/Hudi).

## Alternativas consideradas
| Opção | Por que não |
|---|---|
| Um arquivo por lote (`batch=<hash>.parquet`) | idempotente e barato, mas gera o problema dos arquivos pequenos e não deduplica entre lotes sobrepostos |
| Append puro | duplica em qualquer reexecução |
| Sobrescrever a partição sem mesclar | perde os lotes anteriores do mesmo dia |
