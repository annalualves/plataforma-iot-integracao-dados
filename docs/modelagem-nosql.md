# Modelagem NoSQL

A plataforma usa **persistência poliglota**: cada banco guarda o tipo de dado que ele atende
melhor. A escolha partiu das perguntas que o sistema precisa responder.

## Padrões de acesso

| # | Pergunta | Frequência | Banco |
|---|---|---|---|
| 1 | Qual a configuração da máquina X e dos seus sensores? | baixa, leitura | MongoDB |
| 2 | Quais máquinas usam a peça P? | baixa, leitura | MongoDB / Neo4j |
| 3 | Quais as leituras do sensor S no dia D? | alta, escrita contínua | Cassandra |
| 4 | O que a máquina X mediu no dia D? | média | Cassandra |
| 5 | Quais leituras saíram do normal na linha L hoje? | média | Cassandra |
| 6 | Qual o último valor do sensor S, agora? | muito alta, baixa latência | Redis |
| 7 | Quais sensores pararam de enviar? | contínua | Redis |
| 8 | Se a peça P falhar, quais máquinas são afetadas em sequência? | baixa, consulta de caminho | Neo4j |
| 9 | Histórico completo para análise e reprocessamento | em lote | RustFS (Bronze) |

## MongoDB — catálogo de máquinas e sensores

Banco `fabrica`, coleções `linhas`, `maquinas` e `pecas`. Carga em `app/infra/carga_mongo.py`.

```json
{
  "_id": "L1-M02",
  "linha_id": "L1",
  "tipo": "torno_cnc",
  "fabricante": "Romi",
  "ano_fabricacao": 2019,
  "posicao_na_linha": 2,
  "alimenta": ["L1-M03"],
  "sensores": [
    {"sensor_id": "S-L1-M02-TEMP", "tipo": "temperatura", "unidade": "C", "ativo": true,
     "config": {"media_esperada": 54.1, "desvio": 4.0, "intervalo_s": 1,
                "limites": {"warning": 75.0, "error": 85.0}}}
  ],
  "pecas_ids": ["P-001", "P-003"]
}
```

- **Sensores embutidos (embedding):** o sensor não existe sem a máquina, são poucos por máquina (5)
  e são sempre lidos juntos. Um único `find_one` traz tudo.
- **Peças referenciadas:** a mesma peça é usada por várias máquinas e tem dados próprios
  (fornecedor). Embutir duplicaria a peça em cada máquina. A junção, quando necessária, é feita
  com `$lookup`.
- **Índices:** `linha_id`, `sensores.sensor_id` (único) e `pecas_ids`.
- **Fica fora:** as leituras. Um documento por leitura geraria milhões de documentos por dia num
  banco que não foi feito para isso.

## Cassandra — séries temporais das leituras

Keyspace `iot`. Schema em `app/infra/schema_cassandra.cql`.

| Tabela | Chave de partição | Clustering | Atende |
|---|---|---|---|
| `leituras_por_sensor` | `(sensor_id, dia)` | `ts DESC` | pergunta 3 |
| `leituras_por_maquina` | `(maquina_id, dia)` | `ts DESC, sensor_id` | pergunta 4 |
| `leituras_fora_do_normal_por_linha` | `(linha_id, dia)` | `ts DESC, sensor_id` | pergunta 5 |

- **Modelagem orientada a consultas:** uma tabela por pergunta. A mesma leitura é gravada em mais de
  uma tabela (desnormalização), para cada consulta ler uma única partição, sem `ALLOW FILTERING`.
- **O dia na chave de partição** limita o tamanho da partição: 86.400 linhas por sensor por dia,
  em vez de uma partição que cresce para sempre.
- **`ts DESC`** faz as leituras mais recentes virem primeiro, o padrão mais comum.
- **TTL** de 30 dias (90 dias para leituras fora do normal). O histórico completo fica na Bronze.
- **TimeWindowCompactionStrategy:** compactação por janelas de tempo, indicada para séries temporais
  com TTL.
- **Escrita idempotente:** um `INSERT` com a mesma chave sobrescreve a linha (upsert). Reprocessar
  um lote não duplica dados.

## Redis — estado atual

Atualizado pelo consumidor a cada lote. Não precisa de schema.

| Chave | Tipo | Conteúdo | Expiração |
|---|---|---|---|
| `sensor:<id>:ultimo` | hash | valor, unidade, status, ts, máquina | 10 min |
| `sensores:ultimo_visto` | sorted set | sensor → horário da última leitura | — |
| `contagem:<linha>:<status>:<AAAAMMDDHHMM>` | string (contador) | leituras por status no minuto | 2 h |
| `metricas:consumidor_bronze` | hash | mensagens, inválidas, lotes | — |

- O hash com expiração faz um sensor parado "sumir" sozinho do estado atual.
- O sorted set ordenado pelo horário permite listar os sensores há mais tempo sem enviar com um
  único `ZRANGE`.
- **Fica fora:** histórico. O Redis guarda só o agora, em memória.
- Os contadores (`INCR`) não são idempotentes: um lote reprocessado conta duas vezes. Eles são
  indicadores operacionais aproximados. O número oficial vem da Bronze e do Cassandra.

## Neo4j — topologia da fábrica

Carga em `app/infra/carga_neo4j.py`.

```
(:Fabrica)-[:TEM_LINHA]->(:Linha)-[:CONTEM]->(:Maquina)-[:POSSUI]->(:Sensor)
(:Maquina)-[:ALIMENTA]->(:Maquina)
(:Maquina)-[:USA_PECA]->(:Peca)
```

- **5 tipos de nó e 5 tipos de relação**, com nomes do domínio: o grafo se lê como uma frase.
- `ALIMENTA` representa o fluxo de material. É por ele que se descobre o impacto de uma falha.
- **Restrições de unicidade** no `id` de cada tipo de nó. A carga usa `MERGE`, então pode ser
  repetida sem duplicar nada.
- **Fica fora:** leituras e configurações detalhadas. O grafo guarda a estrutura.

Consultas principais:

```cypher
// sensores de uma linha
MATCH (:Linha {id: 'L1'})-[:CONTEM]->(m:Maquina)-[:POSSUI]->(s:Sensor)
RETURN m.id AS maquina, collect(s.tipo) AS sensores ORDER BY maquina;

// máquinas impactadas se a peça P-003 falhar, até 3 níveis à frente no fluxo
MATCH (p:Peca {id: 'P-003'})<-[:USA_PECA]-(m:Maquina)
OPTIONAL MATCH (m)-[:ALIMENTA*1..3]->(d:Maquina)
RETURN m.id AS usa_a_peca, collect(DISTINCT d.id) AS impactadas_em_seguida;
```

## RustFS — camada Bronze

Buckets `bronze`, `silver` e `gold` (os dois últimos para a Entrega 2). A Bronze guarda tudo o que
passou pelo Kafka, sem transformação, e é a fonte para reprocessar qualquer outra camada.
Ver [ADR 0002](adr/0002-rustfs-como-object-storage.md) e [ADR 0003](adr/0003-formato-e-particionamento-da-bronze.md).
