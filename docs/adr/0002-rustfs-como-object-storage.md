# ADR 0002 — RustFS como object storage do data lake

- **Status:** aceita
- **Data:** 2026-11-24

## Contexto

As camadas Bronze, Silver e Gold precisam de um armazenamento de objetos compatível com S3 que
rode localmente no Docker. A opção mais conhecida e citada nas aulas era o MinIO, mas em outubro
de 2025 o projeto parou de publicar imagens Docker e binários da edição comunitária, e o
repositório entrou em modo de manutenção. `docker pull minio/minio` deixou de funcionar.

O trabalho de Processamento de Dados Massivos, cuja estrutura reaproveitamos, usa HDFS.

## Decisão

Usar o **RustFS** (`rustfs/rustfs`), um armazenamento de objetos de código aberto (licença
Apache 2.0) compatível com a API do S3, com console web.

O código acessa o armazenamento só pela API S3 (boto3 agora, `s3a://` no Spark depois), com
endereço configurável (`S3_ENDPOINT`).

## Consequências

- Sem dependência de imagens não oficiais do MinIO nem de compilar o MinIO a partir do código.
- Como o acesso é pela API S3, trocar por AWS S3, Google Cloud Storage (modo interoperável) ou outro
  serviço compatível exige só mudar endereço e credenciais.
- O RustFS é um projeto mais novo que o MinIO, com menos material de referência. Se surgir
  incompatibilidade, a troca é barata pelo mesmo motivo acima.
- O cliente boto3 está configurado com endereçamento por caminho (`path`) e com o cálculo de
  checksum só quando exigido, para evitar incompatibilidades com servidores S3 que não são da AWS.

## Alternativas consideradas

- **Imagem antiga ou espelho não oficial do MinIO:** sem atualizações de segurança e com origem
  difícil de verificar.
- **HDFS (como no trabalho de PDM):** exige NameNode e DataNode, consome mais memória e não usa a
  API S3, que é o padrão de object storage visto na disciplina.
- **SeaweedFS ou Garage:** também compatíveis com S3, mas com configuração mais trabalhosa.
