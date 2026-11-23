"""Gera catalogo/fabrica.json: a fábrica simulada usada por todo o projeto.

O simulador e as cargas do MongoDB e do Neo4j leem esse mesmo arquivo, então
os IDs batem em todos os bancos. A geração usa semente fixa: rodar de novo
produz exatamente o mesmo arquivo.

Uso (dentro do container):
    python -m catalogo.gerar_catalogo
"""
import json
import random
from pathlib import Path

SEED = 2026

LINHAS = [
    {"id": "L1", "nome": "Linha 1 - Usinagem", "setor": "A"},
    {"id": "L2", "nome": "Linha 2 - Montagem", "setor": "B"},
]

# (sufixo, tipo de máquina) na ordem do fluxo de produção de cada linha
MAQUINAS_POR_LINHA = {
    "L1": [("M01", "serra_fita"), ("M02", "torno_cnc"), ("M03", "fresadora_cnc"),
           ("M04", "retifica"), ("M05", "lavadora_industrial")],
    "L2": [("M01", "esteira_alimentadora"), ("M02", "prensa_hidraulica"), ("M03", "robo_solda"),
           ("M04", "cabine_pintura"), ("M05", "estacao_inspecao")],
}

FABRICANTES = ["Romi", "WEG", "Bosch Rexroth", "Siemens", "ABB", "Mazak"]

# tipo: unidade, média normal, desvio normal, limite de alerta (warning), limite crítico (error)
TIPOS_SENSOR = {
    "temperatura": {"unidade": "C", "media": 55.0, "desvio": 4.0, "warning": 75.0, "error": 85.0},
    "vibracao": {"unidade": "mm/s", "media": 2.5, "desvio": 0.5, "warning": 7.0, "error": 11.0},
    "pressao": {"unidade": "bar", "media": 6.0, "desvio": 0.25, "warning": 7.2, "error": 7.8},
    "umidade": {"unidade": "%", "media": 45.0, "desvio": 3.0, "warning": 65.0, "error": 75.0},
    "corrente": {"unidade": "A", "media": 18.0, "desvio": 1.5, "warning": 26.0, "error": 30.0},
}

PECAS = [
    ("P-001", "Rolamento 6205", "rolamento"),
    ("P-002", "Correia dentada HTD", "correia"),
    ("P-003", "Motor de indução 5 cv", "motor"),
    ("P-004", "Válvula solenoide 1/2", "valvula"),
    ("P-005", "Inversor de frequência", "eletronica"),
    ("P-006", "Bomba hidráulica de engrenagem", "bomba"),
]


def gerar(seed: int = SEED) -> dict:
    rng = random.Random(seed)
    maquinas, sensores = [], []

    for linha in LINHAS:
        ids_linha = [f"{linha['id']}-{suf}" for suf, _ in MAQUINAS_POR_LINHA[linha["id"]]]
        for posicao, ((suf, tipo), maq_id) in enumerate(zip(MAQUINAS_POR_LINHA[linha["id"]], ids_linha)):
            proxima = ids_linha[posicao + 1] if posicao + 1 < len(ids_linha) else None
            maquinas.append({
                "id": maq_id,
                "linha_id": linha["id"],
                "tipo": tipo,
                "fabricante": rng.choice(FABRICANTES),
                "ano_fabricacao": rng.randint(2012, 2024),
                "posicao_na_linha": posicao + 1,
                "alimenta": [proxima] if proxima else [],
            })
            for tipo_sensor, ref in TIPOS_SENSOR.items():
                # cada sensor tem uma média um pouco diferente, como na vida real
                media = round(ref["media"] * rng.uniform(0.95, 1.05), 2)
                sensores.append({
                    "id": f"S-{maq_id}-{tipo_sensor[:4].upper()}",
                    "maquina_id": maq_id,
                    "linha_id": linha["id"],
                    "setor": linha["setor"],
                    "tipo": tipo_sensor,
                    "unidade": ref["unidade"],
                    "media": media,
                    "desvio": ref["desvio"],
                    "limites": {"warning": ref["warning"], "error": ref["error"]},
                    "intervalo_s": 1,
                })

    # o fim da linha de usinagem alimenta o início da linha de montagem
    for m in maquinas:
        if m["id"] == "L1-M05":
            m["alimenta"].append("L2-M01")

    pecas = []
    ids_maquinas = [m["id"] for m in maquinas]
    for pid, nome, tipo in PECAS:
        usadas_em = sorted(rng.sample(ids_maquinas, k=rng.randint(2, 4)))
        pecas.append({"id": pid, "nome": nome, "tipo": tipo,
                      "fornecedor": rng.choice(["Fornecedor A", "Fornecedor B", "Fornecedor C"]),
                      "maquinas": usadas_em})

    return {
        "fabrica": {"id": "FAB-01", "nome": "Fábrica Simulada - Contagem/MG"},
        "linhas": LINHAS,
        "maquinas": maquinas,
        "sensores": sensores,
        "pecas": pecas,
    }


def main() -> None:
    destino = Path(__file__).resolve().parent / "fabrica.json"
    catalogo = gerar()
    destino.write_text(json.dumps(catalogo, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Catálogo gravado em {destino}: {len(catalogo['maquinas'])} máquinas, "
          f"{len(catalogo['sensores'])} sensores, {len(catalogo['pecas'])} peças")


if __name__ == "__main__":
    main()
