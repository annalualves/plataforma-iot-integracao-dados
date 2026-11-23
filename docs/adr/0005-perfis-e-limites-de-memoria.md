# ADR 0005 — Perfis do Docker Compose e limites de memória

- **Status:** aceita
- **Data:** 2026-11-24

## Contexto

O ambiente precisa rodar em notebooks de 8 GB de RAM. Por padrão, o Docker Desktop no Windows
(WSL 2) usa até metade da memória, cerca de 4 GB. Kafka, Cassandra e Neo4j rodam na JVM e, sem
limites, reservariam memória suficiente para esgotar esse espaço sozinhos.

## Decisão

- Dividir os serviços em dois **perfis** do Docker Compose, que sobem separadamente:
  - `ingestao`: RustFS, Kafka, Cassandra, Redis, console do Kafka, init, simulador e consumidor;
  - `catalogo`: MongoDB, Neo4j, Mongo Express e carga do catálogo.
- Definir `mem_limit` em todos os serviços e reduzir a memória das JVMs:
  Kafka com heap de 384 MB, Cassandra com 512 MB, Neo4j com 512 MB e page cache de 128 MB,
  MongoDB com cache de 256 MB e Redis com no máximo 64 MB.
- Um terceiro perfil, `ferramentas`, para rodar consultas e testes sob demanda.

## Consequências

- Cada perfil cabe com folga nos ~4 GB do Docker Desktop.
- Os dois perfis podem rodar juntos em máquinas com 16 GB.
- Com pouca memória, o Cassandra demora mais para iniciar (1 a 3 minutos). Os scripts esperam com
  novas tentativas em vez de falhar.
- Para a demonstração completa numa máquina de 8 GB, os perfis são mostrados um de cada vez.
- Os limites são adequados ao volume simulado (50 sensores), não a um ambiente de produção.
