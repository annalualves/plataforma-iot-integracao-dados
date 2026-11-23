"""Consultas de exemplo em cada banco, para a demonstração e o relatório.

Uso:
    docker compose run --rm ferramentas python -m consultas.consultas_exemplo --banco cassandra
    (bancos: cassandra, redis, mongo, neo4j)

Cassandra e Redis precisam do perfil "ingestao" rodando; MongoDB e Neo4j, do perfil "catalogo".
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from comum import config


def titulo(texto: str) -> None:
    print(f"\n=== {texto} ===")


def mostrar(linhas) -> None:
    for l in linhas:
        print("  " + json.dumps(l, ensure_ascii=False, default=str))


def cassandra() -> None:
    from cassandra.cluster import Cluster

    sessao = Cluster([config.CASSANDRA_HOST]).connect(config.CASSANDRA_KEYSPACE)
    hoje = datetime.now(timezone.utc).date()

    titulo("Últimas 10 leituras do sensor S-L1-M02-TEMP hoje (leituras_por_sensor)")
    cql = ("SELECT ts, valor, unidade, status FROM leituras_por_sensor "
           "WHERE sensor_id = %s AND dia = %s LIMIT 10")
    print(f"  CQL: {cql}")
    mostrar([r._asdict() for r in sessao.execute(cql, ["S-L1-M02-TEMP", hoje])])

    titulo("Últimas 10 leituras de todos os sensores da máquina L2-M03 hoje (leituras_por_maquina)")
    cql = ("SELECT ts, sensor_id, valor, status FROM leituras_por_maquina "
           "WHERE maquina_id = %s AND dia = %s LIMIT 10")
    print(f"  CQL: {cql}")
    mostrar([r._asdict() for r in sessao.execute(cql, ["L2-M03", hoje])])

    titulo("Leituras fora do normal na linha L1 hoje (leituras_fora_do_normal_por_linha)")
    cql = ("SELECT ts, sensor_id, valor, status FROM leituras_fora_do_normal_por_linha "
           "WHERE linha_id = %s AND dia = %s LIMIT 15")
    print(f"  CQL: {cql}")
    mostrar([r._asdict() for r in sessao.execute(cql, ["L1", hoje])])
    sessao.cluster.shutdown()


def redis_() -> None:
    import redis

    r = redis.Redis(host=config.REDIS_HOST, decode_responses=True)

    titulo("Último valor conhecido dos sensores da máquina L1-M02 (HGETALL sensor:<id>:ultimo)")
    for tipo in ("TEMP", "VIBR", "PRES", "UMID", "CORR"):
        sid = f"S-L1-M02-{tipo}"
        print(f"  {sid}: {r.hgetall(f'sensor:{sid}:ultimo')}")

    titulo("Sensores há mais tempo sem enviar (ZRANGE sensores:ultimo_visto 0 4)")
    agora = datetime.now(timezone.utc).timestamp()
    for sid, visto in r.zrange("sensores:ultimo_visto", 0, 4, withscores=True):
        print(f"  {sid}: última leitura há {agora - visto:.0f} s")

    titulo("Contagem de leituras por status no último minuto completo")
    minuto = datetime.fromtimestamp(agora - 60, tz=timezone.utc).strftime("%Y%m%d%H%M")
    for linha in ("L1", "L2"):
        contagens = {s: r.get(f"contagem:{linha}:{s}:{minuto}") or 0 for s in ("ok", "warning", "error")}
        print(f"  {linha} ({minuto} UTC): {contagens}")

    titulo("Métricas do consumidor (HGETALL metricas:consumidor_bronze)")
    print(f"  {r.hgetall('metricas:consumidor_bronze')}")


def mongo() -> None:
    from pymongo import MongoClient

    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]

    titulo("Máquina L1-M02 com seus sensores embutidos (find_one)")
    maq = db.maquinas.find_one({"_id": "L1-M02"}, {"sensores.config": 0})
    print(json.dumps(maq, ensure_ascii=False, indent=2, default=str))

    titulo("Configuração de um sensor pelo ID (busca dentro do array embutido)")
    doc = db.maquinas.find_one({"sensores.sensor_id": "S-L2-M03-TEMP"},
                               {"_id": 1, "sensores.$": 1})
    print(json.dumps(doc, ensure_ascii=False, indent=2, default=str))

    titulo("Máquinas que usam a peça P-003, com os dados da peça ($lookup na coleção referenciada)")
    pipeline = [
        {"$match": {"pecas_ids": "P-003"}},
        {"$lookup": {"from": "pecas", "localField": "pecas_ids", "foreignField": "_id", "as": "pecas"}},
        {"$project": {"tipo": 1, "linha_id": 1,
                      "pecas": {"$filter": {"input": "$pecas", "cond": {"$eq": ["$$this._id", "P-003"]}}}}},
    ]
    mostrar(list(db.maquinas.aggregate(pipeline)))

    titulo("Quantidade de sensores por tipo ($unwind + $group)")
    mostrar(list(db.maquinas.aggregate([
        {"$unwind": "$sensores"},
        {"$group": {"_id": "$sensores.tipo", "total": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ])))


def neo4j() -> None:
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASSWORD))
    consultas = [
        ("Sensores da linha L1",
         "MATCH (:Linha {id: 'L1'})-[:CONTEM]->(m:Maquina)-[:POSSUI]->(s:Sensor) "
         "RETURN m.id AS maquina, collect(s.tipo) AS sensores ORDER BY maquina"),
        ("Máquinas impactadas se a peça P-003 falhar (até 3 níveis à frente no fluxo)",
         "MATCH (p:Peca {id: 'P-003'})<-[:USA_PECA]-(m:Maquina) "
         "OPTIONAL MATCH caminho = (m)-[:ALIMENTA*1..3]->(d:Maquina) "
         "RETURN m.id AS usa_a_peca, collect(DISTINCT d.id) AS impactadas_em_seguida ORDER BY usa_a_peca"),
        ("Caminho do material da primeira máquina da L1 até a última da L2",
         "MATCH caminho = shortestPath((a:Maquina {id: 'L1-M01'})-[:ALIMENTA*]->(b:Maquina {id: 'L2-M05'})) "
         "RETURN [n IN nodes(caminho) | n.id] AS sequencia, length(caminho) AS passos"),
        ("Peças mais compartilhadas (pontos únicos de falha)",
         "MATCH (p:Peca)<-[:USA_PECA]-(m:Maquina) "
         "RETURN p.id AS peca, p.nome AS nome, count(m) AS maquinas ORDER BY maquinas DESC"),
    ]
    with driver.session() as sessao:
        for nome, cypher in consultas:
            titulo(nome)
            print(f"  Cypher: {cypher}")
            mostrar(sessao.run(cypher).data())
    driver.close()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--banco", choices=["cassandra", "redis", "mongo", "neo4j"], required=True)
    banco = p.parse_args().banco
    {"cassandra": cassandra, "redis": redis_, "mongo": mongo, "neo4j": neo4j}[banco]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
