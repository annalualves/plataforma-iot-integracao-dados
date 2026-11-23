# ADR 0003 — Formato e particionamento da camada Bronze

- **Status:** aceita
- **Data:** 2026-11-24

## Contexto

A Bronze é a cópia fiel de tudo o que chegou, para auditoria e para reprocessar Silver e Gold.
Ela precisa guardar inclusive mensagens malformadas, permitir saber de onde veio cada registro
e ser lida em lote pelo Spark na Entrega 2.

## Decisão

- **Formato:** JSON Lines (`.jsonl`), um registro por linha, com a mensagem original em `dado`
  (ou em `bruto`, como texto, se não for JSON válido) e metadados do Kafka em `_kafka`.
- **Sem transformação:** nada é corrigido, filtrado ou descartado na Bronze.
- **Particionamento:** `dt=AAAA-MM-DD/hora=HH`, pelo horário da leitura (`ts`). Se a mensagem não
  tiver um `ts` válido, usa o horário do Kafka; o campo `_particionado_por` registra qual foi usado.
- **Nome do arquivo:** `p<partição>-<offset inicial>-<offset final>.jsonl`, determinístico.
- **Tamanho dos arquivos:** lotes de até 2000 mensagens ou 60 segundos.

## Consequências

- O JSONL preserva a mensagem exatamente como veio e aceita mudanças de schema sem quebrar.
- O particionamento por dia e hora permite ao Spark ler só o período necessário.
- O nome determinístico faz o reprocessamento do mesmo lote sobrescrever o arquivo em vez de
  duplicar (ADR 0004).
- JSON ocupa mais espaço e é mais lento de ler que Parquet. A conversão para Parquet (ou Delta/
  Iceberg) fica para a Silver, na Entrega 2.
- Lotes de 60 s geram muitos arquivos pequenos (cerca de 1.440 por dia por partição). Aceitável
  neste volume; a Silver compacta.

## Alternativas consideradas

- **Parquet já na Bronze:** exigiria definir o schema na entrada e perderia mensagens malformadas.
- **Particionar pelo horário de ingestão:** mais simples, mas leituras atrasadas cairiam na
  partição errada.
