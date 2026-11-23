# ADR 0001 — Persistência poliglota: MongoDB, Cassandra, Redis e Neo4j

- **Status:** aceita
- **Data:** 2026-11-24

## Contexto

A plataforma lida com quatro tipos de dado bem diferentes:

- **catálogo** (máquinas, sensores, configurações): poucos registros, estrutura que varia por tipo
  de máquina, lidos com frequência e alterados raramente;
- **leituras**: alto volume de escrita contínua (50 sensores × 1 leitura/s ≈ 4,3 milhões/dia),
  sempre consultadas por sensor ou máquina e por intervalo de tempo;
- **estado atual** (último valor, sensores parados): leitura e escrita constantes com latência baixa;
- **topologia** (linhas, fluxo entre máquinas, peças): perguntas de caminho e impacto.

Nenhum banco sozinho atende bem aos quatro.

## Decisão

| Dado | Banco | Por quê |
|---|---|---|
| Catálogo | **MongoDB** | Documentos flexíveis; sensores embutidos na máquina, lidos numa só operação |
| Leituras | **Cassandra** | Escrita muito rápida, modelagem por consulta, partição por sensor e dia, TTL |
| Estado atual | **Redis** | Em memória, latência de microssegundos, expiração automática de chaves |
| Topologia | **Neo4j** | Consultas de caminho (`ALIMENTA*1..3`) que exigiriam várias junções em outros bancos |
| Histórico bruto | **RustFS** (S3) | Armazenamento barato e imutável, base para Silver e Gold (ver ADR 0002) |

## Consequências

- Cada banco responde às suas perguntas de forma simples e rápida.
- O mesmo identificador (`sensor_id`, `maquina_id`) aparece em vários bancos. A consistência é
  garantida por todos lerem o mesmo `fabrica.json` e as mesmas mensagens do Kafka.
- Mais serviços para operar e mais memória. Mitigado pelos perfis do Docker Compose (ADR 0005).
- Não há transação entre bancos: se a gravação no Cassandra falhar depois da Bronze, o lote é
  reprocessado inteiro (ADR 0004).

## Alternativas consideradas

- **Só PostgreSQL (com TimescaleDB):** atenderia catálogo e séries temporais, mas não exercitaria
  os modelos NoSQL da disciplina e seria mais fraco nas consultas de grafo.
- **Só MongoDB (incluindo time series collections):** simplificaria a operação, mas as perguntas de
  impacto em cadeia ficariam complexas e o estado atual teria latência maior que no Redis.
