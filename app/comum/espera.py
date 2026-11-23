"""Espera um serviço ficar disponível, tentando de novo com intervalo fixo.

Os bancos demoram para subir (o Cassandra pode levar 1 a 3 minutos num PC com
pouca memória), então todo script que se conecta a eles usa esta função.
"""
import time
from typing import Callable, TypeVar

from comum.log import get_logger

T = TypeVar("T")
log = get_logger("espera")


def aguardar(nome: str, tentar: Callable[[], T], tentativas: int = 60, intervalo: float = 5.0) -> T:
    ultimo_erro = None
    for n in range(1, tentativas + 1):
        try:
            resultado = tentar()
            log.info("servico disponivel", extra={"dados": {"alvo": nome, "tentativa": n}})
            return resultado
        except Exception as erro:  # noqa: BLE001 - qualquer falha de conexão conta como "ainda não"
            ultimo_erro = erro
            log.info("aguardando servico", extra={"dados": {"alvo": nome, "tentativa": n, "erro": str(erro)[:200]}})
            time.sleep(intervalo)
    raise RuntimeError(f"{nome} não ficou disponível após {tentativas} tentativas: {ultimo_erro}")
