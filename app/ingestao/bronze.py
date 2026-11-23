"""Regras da camada Bronze, sem dependência de Kafka ou S3 (para poder ser testada).

A Bronze guarda o dado BRUTO: nada é corrigido nem descartado. Cada linha do
arquivo JSONL tem a mensagem original e os metadados de onde ela veio no Kafka.

Caminho dos arquivos no bucket:
    sensores/leituras/dt=AAAA-MM-DD/hora=HH/p<partição>-<offset inicial>-<offset final>.jsonl

O nome do arquivo vem da partição e dos offsets do Kafka. Se o mesmo lote for
reprocessado, ele sobrescreve o mesmo arquivo em vez de criar uma cópia.
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

PREFIXO = "sensores/leituras"


@dataclass(frozen=True)
class MensagemKafka:
    topico: str
    particao: int
    offset: int
    chave: str | None
    valor: bytes
    kafka_ts_ms: int | None


def interpretar(valor: bytes) -> tuple[dict | None, str | None]:
    """Tenta ler a mensagem como JSON. Devolve (dado, erro)."""
    try:
        dado = json.loads(valor.decode("utf-8"))
        if not isinstance(dado, dict):
            return None, "mensagem não é um objeto JSON"
        return dado, None
    except (UnicodeDecodeError, json.JSONDecodeError) as erro:
        return None, f"json inválido: {erro}"


def instante_do_evento(dado: dict | None, msg: MensagemKafka, ingestao: datetime) -> tuple[datetime, str]:
    """Instante usado para particionar: o ts da leitura; se não der, o do Kafka; senão, o da ingestão."""
    if dado and isinstance(dado.get("ts"), str):
        try:
            return datetime.fromisoformat(dado["ts"].replace("Z", "+00:00")).astimezone(timezone.utc), "evento"
        except ValueError:
            pass
    if msg.kafka_ts_ms:
        return datetime.fromtimestamp(msg.kafka_ts_ms / 1000, tz=timezone.utc), "kafka"
    return ingestao, "ingestao"


def registro_bronze(msg: MensagemKafka, ingestao: datetime) -> tuple[datetime, dict]:
    dado, erro = interpretar(msg.valor)
    instante, origem_instante = instante_do_evento(dado, msg, ingestao)
    registro = {
        "dado": dado,
        "_kafka": {"topico": msg.topico, "particao": msg.particao, "offset": msg.offset,
                   "chave": msg.chave, "ts_ms": msg.kafka_ts_ms},
        "_ingestao_ts": ingestao.isoformat(timespec="milliseconds"),
        "_particionado_por": origem_instante,
    }
    if erro:
        # a mensagem que não deu para ler é guardada como texto, nunca descartada
        registro["bruto"] = msg.valor.decode("utf-8", errors="replace")
        registro["_erro_parse"] = erro
    return instante, registro


def montar_arquivos(mensagens: list[MensagemKafka], ingestao: datetime) -> dict[str, list[dict]]:
    """Agrupa um lote de mensagens em arquivos da Bronze: {chave_no_bucket: [registros]}."""
    grupos: dict[tuple[int, str, str], list[tuple[int, dict]]] = defaultdict(list)
    for msg in mensagens:
        instante, reg = registro_bronze(msg, ingestao)
        grupos[(msg.particao, instante.strftime("%Y-%m-%d"), instante.strftime("%H"))].append((msg.offset, reg))

    arquivos = {}
    for (particao, dia, hora), itens in sorted(grupos.items()):
        itens.sort(key=lambda x: x[0])
        primeiro, ultimo = itens[0][0], itens[-1][0]
        chave = f"{PREFIXO}/dt={dia}/hora={hora}/p{particao}-{primeiro:012d}-{ultimo:012d}.jsonl"
        arquivos[chave] = [reg for _, reg in itens]
    return arquivos


def para_jsonl(registros: list[dict]) -> bytes:
    return ("\n".join(json.dumps(r, ensure_ascii=False) for r in registros) + "\n").encode("utf-8")
