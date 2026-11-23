from datetime import datetime, timedelta, timezone

from catalogo.gerar_catalogo import gerar
from comum.catalogo import carregar_catalogo
from comum.esquema import validar_leitura
from simulador.gerador import GeradorLeituras, classificar_status

INICIO = datetime(2026, 11, 24, 10, 0, 0, tzinfo=timezone.utc)


def _rodadas(gerador, n):
    saida = []
    for i in range(n):
        saida.extend(gerador.rodada(INICIO + timedelta(seconds=i)))
    return saida


def test_catalogo_versionado_igual_ao_gerado():
    # se alguém mudar o gerador, precisa regenerar o fabrica.json
    assert carregar_catalogo() == gerar()


def test_catalogo_tem_ids_unicos_e_referencias_validas():
    cat = carregar_catalogo()
    ids_maquinas = {m["id"] for m in cat["maquinas"]}
    ids_sensores = [s["id"] for s in cat["sensores"]]
    assert len(ids_sensores) == len(set(ids_sensores)) == 50
    assert all(s["maquina_id"] in ids_maquinas for s in cat["sensores"])
    assert all(set(p["maquinas"]) <= ids_maquinas for p in cat["pecas"])
    assert all(set(m["alimenta"]) <= ids_maquinas for m in cat["maquinas"])


def test_todas_as_leituras_respeitam_o_schema():
    gerador = GeradorLeituras(carregar_catalogo()["sensores"], seed=1, prob_anomalia=0.05)
    leituras = _rodadas(gerador, 120)
    assert leituras
    for leitura in leituras:
        assert validar_leitura(leitura) == [], leitura


def test_mesma_semente_gera_mesma_sequencia():
    sensores = carregar_catalogo()["sensores"]
    a = _rodadas(GeradorLeituras(sensores, seed=7, prob_anomalia=0.02), 30)
    b = _rodadas(GeradorLeituras(sensores, seed=7, prob_anomalia=0.02), 30)
    assert a == b


def test_sem_anomalia_os_valores_ficam_perto_da_media():
    sensores = carregar_catalogo()["sensores"]
    leituras = _rodadas(GeradorLeituras(sensores, seed=3, prob_anomalia=0.0), 300)
    assert all(l["anomalia_injetada"] is None for l in leituras)
    assert all(l["status"] == "ok" for l in leituras)
    assert len(leituras) == 300 * len(sensores)  # nenhum sensor some sem anomalia


def test_anomalias_sao_injetadas_e_marcadas():
    sensores = carregar_catalogo()["sensores"]
    leituras = _rodadas(GeradorLeituras(sensores, seed=5, prob_anomalia=0.2), 200)
    tipos = {l["anomalia_injetada"] for l in leituras if l["anomalia_injetada"]}
    assert {"pico", "travado", "deriva"} <= tipos
    # "ausente" não gera mensagem: aparece como leituras faltando
    assert len(leituras) < 200 * len(sensores)
    assert any(l["status"] == "error" for l in leituras if l["anomalia_injetada"] == "pico")


def test_classificar_status():
    limites = {"warning": 75.0, "error": 85.0}
    assert classificar_status(60, limites) == "ok"
    assert classificar_status(75, limites) == "warning"
    assert classificar_status(90, limites) == "error"


def test_validar_leitura_aponta_problemas():
    problemas = validar_leitura({"sensor_id": 1, "status": "quebrado", "ts": "ontem"})
    assert "campo ausente: valor" in problemas
    assert "tipo inválido em sensor_id" in problemas
    assert "status inválido" in problemas
    assert "ts não é ISO 8601" in problemas
