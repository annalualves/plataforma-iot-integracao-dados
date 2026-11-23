"""Mostra o que já chegou na camada Bronze (bucket "bronze" no RustFS).

Uso:
    docker compose run --rm ferramentas python -m consultas.verificar_bronze
"""
from __future__ import annotations

import json
import sys
from collections import Counter

from comum import config
from ingestao.bronze import PREFIXO
from ingestao.consumidor_bronze import criar_s3


def main() -> int:
    s3 = criar_s3()
    objetos = []
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=config.BUCKET_BRONZE, Prefix=PREFIXO):
        objetos.extend(pagina.get("Contents", []))

    if not objetos:
        print("A Bronze ainda está vazia. O consumidor grava um lote a cada 60 s (ou 2000 mensagens).")
        return 1

    objetos.sort(key=lambda o: o["Key"])
    por_hora = Counter("/".join(o["Key"].split("/")[2:4]) for o in objetos)
    total_bytes = sum(o["Size"] for o in objetos)
    print(f"Arquivos na Bronze: {len(objetos)} ({total_bytes / 1024:.1f} KB)")
    print("Arquivos por partição (dt/hora):")
    for particao, n in sorted(por_hora.items()):
        print(f"  {particao}: {n}")

    ultimo = objetos[-1]["Key"]
    corpo = s3.get_object(Bucket=config.BUCKET_BRONZE, Key=ultimo)["Body"].read().decode("utf-8")
    linhas = [json.loads(l) for l in corpo.splitlines() if l.strip()]
    status = Counter((l.get("dado") or {}).get("status", "ilegivel") for l in linhas)
    anomalias = Counter((l.get("dado") or {}).get("anomalia_injetada") for l in linhas)
    print(f"\nÚltimo arquivo: {ultimo}")
    print(f"  registros: {len(linhas)} | status: {dict(status)} | anomalias injetadas: "
          f"{ {k: v for k, v in anomalias.items() if k} }")
    print("  exemplo de registro:")
    print(json.dumps(linhas[0], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
