# ADR-0001 — SeaweedFS como object storage S3

**Status:** aceito · **Entrega:** 1

## Contexto
O lakehouse precisa de um object storage compatível com S3 para as camadas
Bronze/Silver/Gold. O MinIO, usado nas aulas, teve a edição comunitária
arquivada em 2026 e não publica mais imagens Docker.

## Decisão
Usar **SeaweedFS 4.47** (`chrislusf/seaweedfs`), que expõe a mesma API S3.

## Consequências
- O código cliente não muda: `boto3` hoje, `s3a://` no Spark da Entrega 2.
- Duas incompatibilidades conhecidas, já tratadas:
  - o SeaweedFS não implementa os checksums novos do SDK AWS, então o cliente
    usa `request_checksum_calculation="when_required"` (idem no `aws-cli` do
    serviço de bootstrap);
  - o endereçamento é *path-style*, não *virtual-host*.
- Trocar por Garage ou RustFS depois custa apenas a edição do Compose.

## Alternativas consideradas
| Opção | Por que não |
|---|---|
| MinIO | imagens Docker descontinuadas |
| LocalStack S3 | pesado e voltado a teste, não a dado persistente |
| Sistema de arquivos local | não exercita a API S3, que é o alvo da disciplina |
