"""Logs estruturados em JSON (uma linha por evento), para facilitar busca e métricas.

Uso:
    log = get_logger("consumidor")
    log.info("lote gravado", extra={"dados": {"mensagens": 120, "arquivo": "..."}})
"""
import json
import logging
import sys
from datetime import datetime, timezone


class FormatadorJson(logging.Formatter):
    def format(self, registro: logging.LogRecord) -> str:
        evento = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "nivel": registro.levelname,
            "servico": registro.name,
            "msg": registro.getMessage(),
        }
        dados = getattr(registro, "dados", None)
        if isinstance(dados, dict):
            evento.update(dados)
        if registro.exc_info:
            evento["erro"] = self.formatException(registro.exc_info)
        return json.dumps(evento, ensure_ascii=False, default=str)


def get_logger(nome: str) -> logging.Logger:
    log = logging.getLogger(nome)
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(FormatadorJson())
        log.addHandler(handler)
        log.setLevel(logging.INFO)
        log.propagate = False
    return log
