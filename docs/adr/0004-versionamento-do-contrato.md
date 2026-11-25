# ADR-0004 — Contrato versionado (v1 e v2 lado a lado)

**Status:** aceito · **Entrega:** 1

## Contexto
O gabarito do simulador precisava registrar **qual** anomalia foi injetada
(`anomaly_type`), para medir o detector na Entrega 3. O contrato v1 do starter
kit declara `additionalProperties: false`, então acrescentar o campo faria
**toda** leitura ser rejeitada pela validação.

## Decisão
Criar `contracts/sensor_reading.v2.schema.json` (`schema_version: 2`, com
`anomaly_type` opcional) e manter a v1 versionada. A ingestão carrega **um
validador por versão** e escolhe pelo campo `schema_version` da própria leitura.

## Consequências
- Produtores antigos (v1) continuam aceitos sem alteração.
- Uma `schema_version` desconhecida não quebra a ingestão: vai para a quarentena
  com o motivo explícito.
- O mesmo mecanismo serve ao Schema Registry da Entrega 3, que também versiona
  por compatibilidade.
