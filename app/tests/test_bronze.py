import json
from datetime import datetime, timezone

from ingestao.bronze import MensagemKafka, montar_arquivos, para_jsonl

INGESTAO = datetime(2026, 11, 24, 13, 5, 0, tzinfo=timezone.utc)


def _msg(particao, offset, ts="2026-11-24T12:59:59.500Z", valor=None):
    corpo = valor if valor is not None else json.dumps({"sensor_id": "S-1", "ts": ts, "valor": 1.0}).encode()
    return MensagemKafka("sensores.leituras", particao, offset, "S-1", corpo, 1_795_000_000_000)


def test_particiona_por_hora_do_evento_e_por_particao_do_kafka():
    lote = [_msg(0, 10), _msg(0, 11, ts="2026-11-24T13:00:01.000Z"), _msg(2, 5)]
    arquivos = montar_arquivos(lote, INGESTAO)
    assert set(arquivos) == {
        "sensores/leituras/dt=2026-11-24/hora=12/p0-000000000010-000000000010.jsonl",
        "sensores/leituras/dt=2026-11-24/hora=13/p0-000000000011-000000000011.jsonl",
        "sensores/leituras/dt=2026-11-24/hora=12/p2-000000000005-000000000005.jsonl",
    }


def test_reprocessar_o_mesmo_lote_gera_os_mesmos_arquivos():
    lote = [_msg(1, o) for o in range(100, 110)]
    a = montar_arquivos(lote, INGESTAO)
    b = montar_arquivos(list(reversed(lote)), INGESTAO)
    assert list(a) == list(b)
    assert [r["_kafka"]["offset"] for r in a[next(iter(a))]] == list(range(100, 110))


def test_mensagem_invalida_vai_para_a_bronze_como_texto():
    lote = [_msg(0, 1, valor=b"isto nao e json")]
    (registros,) = montar_arquivos(lote, INGESTAO).values()
    reg = registros[0]
    assert reg["dado"] is None
    assert reg["bruto"] == "isto nao e json"
    assert reg["_erro_parse"].startswith("json inválido")
    assert reg["_particionado_por"] == "kafka"


def test_jsonl_tem_uma_linha_por_registro():
    arquivos = montar_arquivos([_msg(0, 1), _msg(0, 2)], INGESTAO)
    conteudo = para_jsonl(next(iter(arquivos.values()))).decode()
    linhas = conteudo.strip().split("\n")
    assert len(linhas) == 2
    assert all(json.loads(l)["_kafka"]["topico"] == "sensores.leituras" for l in linhas)
