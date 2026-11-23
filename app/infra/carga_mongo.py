"""Carrega o catálogo da fábrica no MongoDB. Pode rodar várias vezes (usa upsert).

Modelagem:
- linhas:   um documento por linha de produção
- maquinas: um documento por máquina, com os SENSORES EMBUTIDOS (embedding),
            porque sensor e máquina são sempre lidos juntos e o sensor não
            existe fora da máquina
- pecas:    coleção separada, REFERENCIADA pelas máquinas (pecas_ids), porque
            a mesma peça é usada por várias máquinas e muda independentemente
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

from comum import config
from comum.catalogo import carregar_catalogo
from comum.espera import aguardar
from comum.log import get_logger

log = get_logger("carga_mongo")


def montar_documentos(catalogo: dict) -> dict[str, list[dict]]:
    agora = datetime.now(timezone.utc)
    pecas_por_maquina: dict[str, list[str]] = {}
    for peca in catalogo["pecas"]:
        for maq in peca["maquinas"]:
            pecas_por_maquina.setdefault(maq, []).append(peca["id"])

    sensores_por_maquina: dict[str, list[dict]] = {}
    for s in catalogo["sensores"]:
        sensores_por_maquina.setdefault(s["maquina_id"], []).append({
            "sensor_id": s["id"], "tipo": s["tipo"], "unidade": s["unidade"],
            "config": {"media_esperada": s["media"], "desvio": s["desvio"],
                       "intervalo_s": s["intervalo_s"], "limites": s["limites"]},
            "ativo": True,
        })

    linhas = [{"_id": l["id"], "nome": l["nome"], "setor": l["setor"],
               "fabrica_id": catalogo["fabrica"]["id"], "atualizado_em": agora}
              for l in catalogo["linhas"]]
    maquinas = [{"_id": m["id"], "linha_id": m["linha_id"], "tipo": m["tipo"],
                 "fabricante": m["fabricante"], "ano_fabricacao": m["ano_fabricacao"],
                 "posicao_na_linha": m["posicao_na_linha"], "alimenta": m["alimenta"],
                 "sensores": sensores_por_maquina.get(m["id"], []),
                 "pecas_ids": sorted(pecas_por_maquina.get(m["id"], [])),
                 "atualizado_em": agora}
                for m in catalogo["maquinas"]]
    pecas = [{"_id": p["id"], "nome": p["nome"], "tipo": p["tipo"], "fornecedor": p["fornecedor"],
              "atualizado_em": agora} for p in catalogo["pecas"]]
    return {"linhas": linhas, "maquinas": maquinas, "pecas": pecas}


def main() -> int:
    from pymongo import ASCENDING, MongoClient, ReplaceOne

    cliente = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    aguardar("mongodb", lambda: cliente.admin.command("ping"))
    db = cliente[config.MONGO_DB]

    for colecao, docs in montar_documentos(carregar_catalogo()).items():
        res = db[colecao].bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs])
        log.info("colecao carregada", extra={"dados": {"colecao": colecao, "documentos": len(docs),
                                                      "inseridos": res.upserted_count,
                                                      "atualizados": res.modified_count}})

    # índices para os padrões de acesso usados nas consultas
    db.maquinas.create_index([("linha_id", ASCENDING)])
    db.maquinas.create_index([("sensores.sensor_id", ASCENDING)], unique=True)
    db.maquinas.create_index([("pecas_ids", ASCENDING)])
    log.info("indices criados", extra={"dados": {"colecao": "maquinas"}})
    return 0


if __name__ == "__main__":
    sys.exit(main())
