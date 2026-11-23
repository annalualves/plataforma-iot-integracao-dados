"""Leitura do catálogo da fábrica (catalogo/fabrica.json)."""
import json
from pathlib import Path

from comum.config import CATALOGO_PATH


def carregar_catalogo(caminho: Path = CATALOGO_PATH) -> dict:
    with open(caminho, encoding="utf-8") as f:
        return json.load(f)


def indice_por_id(itens: list[dict]) -> dict[str, dict]:
    return {item["id"]: item for item in itens}
