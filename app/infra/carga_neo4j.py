"""Carrega a topologia da fábrica no Neo4j. Pode rodar várias vezes (usa MERGE).

Nós:      Fabrica, Linha, Maquina, Sensor, Peca
Relações: (Fabrica)-[:TEM_LINHA]->(Linha)
          (Linha)-[:CONTEM]->(Maquina)
          (Maquina)-[:POSSUI]->(Sensor)
          (Maquina)-[:ALIMENTA]->(Maquina)     fluxo de material entre máquinas
          (Maquina)-[:USA_PECA]->(Peca)

As leituras NÃO ficam no Neo4j (ficam no Cassandra). O grafo guarda só a
estrutura, que é o que ele responde bem: "o que é impactado se X falhar?".
"""
from __future__ import annotations

import sys

from comum import config
from comum.catalogo import carregar_catalogo
from comum.espera import aguardar
from comum.log import get_logger

log = get_logger("carga_neo4j")

RESTRICOES = [
    "CREATE CONSTRAINT fabrica_id IF NOT EXISTS FOR (n:Fabrica) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT linha_id IF NOT EXISTS FOR (n:Linha) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT maquina_id IF NOT EXISTS FOR (n:Maquina) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT sensor_id IF NOT EXISTS FOR (n:Sensor) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT peca_id IF NOT EXISTS FOR (n:Peca) REQUIRE n.id IS UNIQUE",
]

CARGAS = [
    ("fabrica e linhas", """
        MERGE (f:Fabrica {id: $fabrica.id}) SET f.nome = $fabrica.nome
        WITH f
        UNWIND $linhas AS l
        MERGE (li:Linha {id: l.id}) SET li.nome = l.nome, li.setor = l.setor
        MERGE (f)-[:TEM_LINHA]->(li)
    """),
    ("maquinas", """
        UNWIND $maquinas AS m
        MERGE (ma:Maquina {id: m.id})
          SET ma.tipo = m.tipo, ma.fabricante = m.fabricante,
              ma.ano_fabricacao = m.ano_fabricacao, ma.posicao_na_linha = m.posicao_na_linha
        WITH ma, m
        MATCH (li:Linha {id: m.linha_id})
        MERGE (li)-[:CONTEM]->(ma)
    """),
    ("fluxo entre maquinas", """
        UNWIND $maquinas AS m
        UNWIND m.alimenta AS destino
        MATCH (a:Maquina {id: m.id}), (b:Maquina {id: destino})
        MERGE (a)-[:ALIMENTA]->(b)
    """),
    ("sensores", """
        UNWIND $sensores AS s
        MERGE (se:Sensor {id: s.id}) SET se.tipo = s.tipo, se.unidade = s.unidade
        WITH se, s
        MATCH (ma:Maquina {id: s.maquina_id})
        MERGE (ma)-[:POSSUI]->(se)
    """),
    ("pecas", """
        UNWIND $pecas AS p
        MERGE (pe:Peca {id: p.id}) SET pe.nome = p.nome, pe.tipo = p.tipo, pe.fornecedor = p.fornecedor
        WITH pe, p
        UNWIND p.maquinas AS maq
        MATCH (ma:Maquina {id: maq})
        MERGE (ma)-[:USA_PECA]->(pe)
    """),
]


def main() -> int:
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASSWORD))
    aguardar("neo4j", driver.verify_connectivity, tentativas=60, intervalo=5)
    catalogo = carregar_catalogo()

    with driver.session() as sessao:
        for cmd in RESTRICOES:
            sessao.run(cmd).consume()
        for nome, cypher in CARGAS:
            resumo = sessao.run(cypher, **catalogo).consume().counters
            log.info("carga concluida", extra={"dados": {"etapa": nome,
                                                        "nos_criados": resumo.nodes_created,
                                                        "relacoes_criadas": resumo.relationships_created}})
        totais = sessao.run("MATCH (n) RETURN labels(n)[0] AS tipo, count(*) AS total ORDER BY tipo").data()
        log.info("grafo pronto", extra={"dados": {"totais": totais}})
    driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
