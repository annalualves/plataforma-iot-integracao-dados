# ADR 0004 — Kafka como barramento de ingestão, com entrega "pelo menos uma vez"

- **Status:** aceita
- **Data:** 2026-11-24

## Contexto

O simulador precisa entregar as leituras a mais de um destino (Bronze, Cassandra, Redis e, na
Entrega 3, o processamento em streaming). Se um destino ficar fora do ar, as leituras não podem
se perder.

## Decisão

- O simulador publica no tópico **`sensores.leituras`** do Kafka (3 partições, retenção de 24 h).
- **Chave da mensagem = `sensor_id`:** as leituras de um sensor ficam na mesma partição, em ordem.
- O produtor usa `acks=all` e `enable.idempotence=true`, para não duplicar mensagens em reenvios.
- O consumidor (grupo `bronze-writer`) usa **commit manual**: confirma os offsets só depois de
  gravar o lote na Bronze, no Cassandra e no Redis.

## Consequências

- Se o consumidor cair, ele volta de onde parou; nada se perde enquanto a mensagem estiver no
  Kafka (24 h).
- A entrega é "pelo menos uma vez": um lote pode ser processado duas vezes. Os destinos lidam
  com isso:
  - **Bronze:** nome de arquivo determinístico (ADR 0003); sobreposições são removidas na Silver
    deduplicando por `(particao, offset)` ou `evento_id`;
  - **Cassandra:** `INSERT` é upsert pela chave primária;
  - **Redis:** o último valor é sobrescrito; os contadores podem contar a mais (são aproximados).
- Na Entrega 3, o streaming lê o mesmo tópico com outro grupo de consumidores, sem afetar a Bronze.
- O Kafka sem volume persistente perde as mensagens ao remover o container. Aceitável: a Bronze
  é o registro permanente.

## Alternativas consideradas

- **Simulador gravando direto nos bancos:** acoplaria a fonte aos destinos; uma falha em um banco
  pararia tudo.
- **Exatamente uma vez (transações do Kafka):** mais complexo e não cobre gravações fora do Kafka,
  como o RustFS e o Cassandra.
