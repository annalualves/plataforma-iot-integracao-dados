"""Consumidor do tópico sensores.leituras.

Para cada lote de mensagens:
  1. grava o dado bruto na Bronze (RustFS, bucket "bronze"), em JSONL;
  2. grava as leituras válidas no Cassandra (séries temporais);
  3. atualiza o estado atual no Redis (último valor, contadores);
  4. só então confirma (commit) os offsets no Kafka.

Como o commit vem por último, se o consumidor cair no meio do caminho o lote é
reprocessado (entrega "pelo menos uma vez"). Isso é seguro: o arquivo da Bronze
tem nome fixo por partição/offset e o Cassandra faz upsert pela chave primária.
"""
from __future__ import annotations

import signal
import sys
import time
from datetime import datetime, timezone

from comum import config
from comum.espera import aguardar
from comum.esquema import validar_leitura
from comum.log import get_logger
from ingestao.bronze import MensagemKafka, montar_arquivos, para_jsonl

log = get_logger("consumidor_bronze")
parar = False

CQL_POR_SENSOR = """
INSERT INTO leituras_por_sensor (sensor_id, dia, ts, valor, unidade, status, tipo, maquina_id, linha_id, evento_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
CQL_POR_MAQUINA = """
INSERT INTO leituras_por_maquina (maquina_id, dia, ts, sensor_id, tipo, valor, unidade, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
CQL_FORA_DO_NORMAL = """
INSERT INTO leituras_fora_do_normal_por_linha (linha_id, dia, ts, sensor_id, maquina_id, tipo, valor, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""


def _tratar_sinal(*_):
    global parar
    parar = True


def criar_s3():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"},
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required",
                      retries={"max_attempts": 5, "mode": "standard"}),
    )


def conectar_cassandra():
    from cassandra.cluster import Cluster

    cluster = Cluster([config.CASSANDRA_HOST])
    return cluster.connect(config.CASSANDRA_KEYSPACE)


def _ts(dado: dict) -> datetime:
    return datetime.fromisoformat(dado["ts"].replace("Z", "+00:00"))


def gravar_cassandra(sessao, stmts, leituras: list[dict]) -> None:
    from cassandra.concurrent import execute_concurrent_with_args

    por_sensor, por_maquina, fora = [], [], []
    for d in leituras:
        ts = _ts(d)
        dia = ts.date()
        por_sensor.append((d["sensor_id"], dia, ts, float(d["valor"]), d["unidade"], d["status"],
                           d["tipo"], d["maquina_id"], d["linha_id"], d["evento_id"]))
        por_maquina.append((d["maquina_id"], dia, ts, d["sensor_id"], d["tipo"], float(d["valor"]),
                            d["unidade"], d["status"]))
        if d["status"] != "ok":
            fora.append((d["linha_id"], dia, ts, d["sensor_id"], d["maquina_id"], d["tipo"],
                         float(d["valor"]), d["status"]))
    for stmt, params in ((stmts["sensor"], por_sensor), (stmts["maquina"], por_maquina), (stmts["fora"], fora)):
        if params:
            resultados = execute_concurrent_with_args(sessao, stmt, params, concurrency=50,
                                                      raise_on_first_error=True)
            for ok, _ in resultados:
                if not ok:
                    raise RuntimeError("falha ao gravar no Cassandra")


def gravar_redis(r, leituras: list[dict]) -> None:
    pipe = r.pipeline(transaction=False)
    for d in sorted(leituras, key=lambda x: x["ts"]):
        chave = f"sensor:{d['sensor_id']}:ultimo"
        pipe.hset(chave, mapping={"valor": d["valor"], "unidade": d["unidade"], "status": d["status"],
                                  "ts": d["ts"], "maquina_id": d["maquina_id"]})
        pipe.expire(chave, 600)
        pipe.zadd("sensores:ultimo_visto", {d["sensor_id"]: _ts(d).timestamp()})
        minuto = _ts(d).strftime("%Y%m%d%H%M")
        contador = f"contagem:{d['linha_id']}:{d['status']}:{minuto}"
        pipe.incr(contador)
        pipe.expire(contador, 7200)
    pipe.execute()


def processar_lote(lote: list[MensagemKafka], s3, sessao, stmts, r) -> dict:
    ingestao = datetime.now(timezone.utc)
    arquivos = montar_arquivos(lote, ingestao)
    for chave, registros in arquivos.items():
        s3.put_object(Bucket=config.BUCKET_BRONZE, Key=chave, Body=para_jsonl(registros),
                      ContentType="application/x-ndjson")

    validas, invalidas = [], 0
    for registros in arquivos.values():
        for reg in registros:
            dado = reg["dado"]
            if dado is not None and not validar_leitura(dado):
                validas.append(dado)
            else:
                invalidas += 1

    gravar_cassandra(sessao, stmts, validas)
    gravar_redis(r, validas)
    r.hincrby("metricas:consumidor_bronze", "mensagens", len(lote))
    r.hincrby("metricas:consumidor_bronze", "invalidas", invalidas)
    r.hincrby("metricas:consumidor_bronze", "lotes", 1)
    return {"mensagens": len(lote), "validas": len(validas), "invalidas": invalidas,
            "arquivos": list(arquivos.keys())}


def main() -> int:
    from confluent_kafka import Consumer, TIMESTAMP_NOT_AVAILABLE
    import redis

    signal.signal(signal.SIGTERM, _tratar_sinal)
    signal.signal(signal.SIGINT, _tratar_sinal)

    s3 = criar_s3()
    aguardar("rustfs", lambda: s3.head_bucket(Bucket=config.BUCKET_BRONZE))
    sessao = aguardar("cassandra", conectar_cassandra)
    stmts = {"sensor": sessao.prepare(CQL_POR_SENSOR), "maquina": sessao.prepare(CQL_POR_MAQUINA),
             "fora": sessao.prepare(CQL_FORA_DO_NORMAL)}
    r = redis.Redis(host=config.REDIS_HOST, decode_responses=True)
    aguardar("redis", r.ping)

    consumidor = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": "bronze-writer",
        "enable.auto.commit": False,      # o commit é manual, depois de gravar
        "auto.offset.reset": "earliest",
    })
    consumidor.subscribe([config.TOPICO_LEITURAS])
    log.info("consumidor iniciado", extra={"dados": {
        "topico": config.TOPICO_LEITURAS, "lote_max_mensagens": config.LOTE_MAX_MENSAGENS,
        "lote_max_segundos": config.LOTE_MAX_SEGUNDOS}})

    lote: list[MensagemKafka] = []
    inicio_lote = time.monotonic()

    def descarregar():
        nonlocal lote, inicio_lote
        if not lote:
            return
        t0 = time.monotonic()
        resumo = processar_lote(lote, s3, sessao, stmts, r)
        consumidor.commit(asynchronous=False)
        resumo["duracao_ms"] = round((time.monotonic() - t0) * 1000)
        resumo["offset_final_por_particao"] = {m.particao: m.offset for m in lote}
        log.info("lote gravado", extra={"dados": resumo})
        lote = []
        inicio_lote = time.monotonic()

    try:
        while not parar:
            msg = consumidor.poll(1.0)
            if msg is not None:
                if msg.error():
                    log.error("erro do kafka", extra={"dados": {"erro": str(msg.error())}})
                    continue
                tipo_ts, ts_ms = msg.timestamp()
                if not lote:
                    inicio_lote = time.monotonic()
                lote.append(MensagemKafka(
                    topico=msg.topic(), particao=msg.partition(), offset=msg.offset(),
                    chave=msg.key().decode("utf-8", errors="replace") if msg.key() else None,
                    valor=msg.value() or b"",
                    kafka_ts_ms=None if tipo_ts == TIMESTAMP_NOT_AVAILABLE else ts_ms,
                ))
            cheio = len(lote) >= config.LOTE_MAX_MENSAGENS
            venceu = lote and time.monotonic() - inicio_lote >= config.LOTE_MAX_SEGUNDOS
            if cheio or venceu:
                descarregar()
        descarregar()  # grava o que sobrou antes de sair
    except Exception:
        log.exception("falha no processamento; o lote não foi confirmado e será reprocessado")
        return 1
    finally:
        consumidor.close()
        log.info("consumidor encerrado")
    return 0


if __name__ == "__main__":
    sys.exit(main())
