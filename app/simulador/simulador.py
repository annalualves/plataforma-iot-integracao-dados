"""Simulador de sensores: publica leituras no Kafka (tópico sensores.leituras).

Uso:
    python -m simulador.simulador                       # envia para o Kafka, sem parar
    python -m simulador.simulador --saida stdout --duracao 5   # só imprime (para testar)

Parâmetros principais:
    --intervalo       segundos entre rodadas (cada rodada = 1 leitura de cada sensor)
    --prob-anomalia   chance de começar uma anomalia, por leitura (0.01 = 1%)
    --seed            semente aleatória; a mesma semente gera a mesma sequência
    --duracao         segundos de execução (0 = infinito)
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone

from comum import config
from comum.catalogo import carregar_catalogo
from comum.log import get_logger
from simulador.gerador import GeradorLeituras

log = get_logger("simulador")
parar = False


def _tratar_sinal(*_):
    global parar
    parar = True


def criar_produtor():
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "client.id": "simulador-sensores",
        "acks": "all",
        "enable.idempotence": True,   # evita duplicar mensagens em reenvios do produtor
        "linger.ms": 50,
        "compression.type": "lz4",
    })


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Simulador de sensores IoT")
    p.add_argument("--intervalo", type=float, default=1.0)
    p.add_argument("--prob-anomalia", type=float, default=0.01)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--duracao", type=float, default=0)
    p.add_argument("--saida", choices=["kafka", "stdout"], default="kafka")
    args = p.parse_args(argv)

    signal.signal(signal.SIGTERM, _tratar_sinal)
    signal.signal(signal.SIGINT, _tratar_sinal)

    catalogo = carregar_catalogo()
    gerador = GeradorLeituras(catalogo["sensores"], seed=args.seed, prob_anomalia=args.prob_anomalia)

    produtor = None
    erros_envio = {"n": 0}
    if args.saida == "kafka":
        produtor = criar_produtor()

        def ao_entregar(erro, _msg):
            if erro is not None:
                erros_envio["n"] += 1
                log.error("falha ao entregar mensagem", extra={"dados": {"erro": str(erro)}})

    log.info("simulador iniciado", extra={"dados": {
        "sensores": len(catalogo["sensores"]), "intervalo_s": args.intervalo,
        "prob_anomalia": args.prob_anomalia, "seed": args.seed, "saida": args.saida,
        "topico": config.TOPICO_LEITURAS}})

    inicio = time.monotonic()
    ultimo_resumo = inicio
    enviadas = anomalias = 0

    while not parar:
        t0 = time.monotonic()
        for leitura in gerador.rodada(datetime.now(timezone.utc)):
            if leitura["anomalia_injetada"]:
                anomalias += 1
            valor = json.dumps(leitura, ensure_ascii=False)
            if produtor is not None:
                # chave = sensor_id: todas as leituras de um sensor caem na mesma partição, em ordem
                produtor.produce(config.TOPICO_LEITURAS, key=leitura["sensor_id"], value=valor,
                                 on_delivery=ao_entregar)
            else:
                print(valor)
            enviadas += 1
        if produtor is not None:
            produtor.poll(0)

        agora = time.monotonic()
        if agora - ultimo_resumo >= 10:
            log.info("resumo", extra={"dados": {"mensagens_enviadas": enviadas,
                                                "leituras_com_anomalia": anomalias,
                                                "erros_envio": erros_envio["n"]}})
            ultimo_resumo = agora
        if args.duracao and agora - inicio >= args.duracao:
            break
        time.sleep(max(0.0, args.intervalo - (time.monotonic() - t0)))

    if produtor is not None:
        pendentes = produtor.flush(10)
        log.info("simulador encerrado", extra={"dados": {"mensagens_enviadas": enviadas,
                                                         "pendentes_no_flush": pendentes}})
    return 0


if __name__ == "__main__":
    sys.exit(main())
