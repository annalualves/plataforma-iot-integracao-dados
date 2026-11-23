"""Prepara a infraestrutura da ingestão. Pode rodar várias vezes sem problema.

1. RustFS: cria os buckets bronze, silver e gold
2. Kafka: cria o tópico sensores.leituras (3 partições, retenção de 24 h)
3. Cassandra: cria o keyspace e as tabelas (infra/schema_cassandra.cql)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

from comum import config
from comum.espera import aguardar
from comum.log import get_logger

log = get_logger("init_infra")
SCHEMA_CQL = Path(__file__).resolve().parent / "schema_cassandra.cql"


def criar_buckets() -> None:
    from ingestao.consumidor_bronze import criar_s3

    s3 = criar_s3()
    existentes = aguardar("rustfs", lambda: {b["Name"] for b in s3.list_buckets().get("Buckets", [])})
    for bucket in config.BUCKETS:
        if bucket in existentes:
            log.info("bucket ja existe", extra={"dados": {"bucket": bucket}})
        else:
            s3.create_bucket(Bucket=bucket)
            log.info("bucket criado", extra={"dados": {"bucket": bucket}})


def criar_topico() -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    topicos = aguardar("kafka", lambda: admin.list_topics(timeout=10).topics)
    if config.TOPICO_LEITURAS in topicos:
        log.info("topico ja existe", extra={"dados": {"topico": config.TOPICO_LEITURAS}})
        return
    novo = NewTopic(config.TOPICO_LEITURAS, num_partitions=3, replication_factor=1,
                    config={"retention.ms": str(24 * 3600 * 1000)})
    for topico, futuro in admin.create_topics([novo]).items():
        futuro.result()
        log.info("topico criado", extra={"dados": {"topico": topico, "particoes": 3}})


def comandos_cql(texto: str) -> list[str]:
    sem_comentarios = re.sub(r"--[^\n]*", "", texto)
    return [c.strip() for c in sem_comentarios.split(";") if c.strip()]


def criar_schema_cassandra() -> None:
    from cassandra.cluster import Cluster

    def conectar():
        # um Cluster novo a cada tentativa: o driver não reaproveita bem uma conexão que falhou
        return Cluster([config.CASSANDRA_HOST]).connect()

    sessao = aguardar("cassandra", conectar, tentativas=60, intervalo=5)
    for cmd in comandos_cql(SCHEMA_CQL.read_text(encoding="utf-8")):
        sessao.execute(cmd, timeout=60)
    tabelas = [r.table_name for r in sessao.execute(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s", [config.CASSANDRA_KEYSPACE])]
    log.info("schema do cassandra pronto", extra={"dados": {"keyspace": config.CASSANDRA_KEYSPACE,
                                                           "tabelas": tabelas}})
    sessao.cluster.shutdown()


def main() -> int:
    criar_buckets()
    criar_topico()
    criar_schema_cassandra()
    log.info("infraestrutura pronta")
    return 0


if __name__ == "__main__":
    sys.exit(main())
