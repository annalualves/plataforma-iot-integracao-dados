from comum.catalogo import carregar_catalogo
from infra.carga_mongo import montar_documentos
from infra.init_infra import SCHEMA_CQL, comandos_cql


def test_documentos_mongo_embutem_sensores_e_referenciam_pecas():
    docs = montar_documentos(carregar_catalogo())
    maquinas = {m["_id"]: m for m in docs["maquinas"]}
    assert len(maquinas) == 10
    assert all(len(m["sensores"]) == 5 for m in maquinas.values())
    ids_pecas = {p["_id"] for p in docs["pecas"]}
    assert all(set(m["pecas_ids"]) <= ids_pecas for m in maquinas.values())


def test_schema_cassandra_tem_keyspace_e_tres_tabelas():
    comandos = comandos_cql(SCHEMA_CQL.read_text(encoding="utf-8"))
    assert comandos[0].startswith("CREATE KEYSPACE")
    tabelas = [c for c in comandos if c.startswith("CREATE TABLE")]
    assert len(tabelas) == 3
    assert all("PRIMARY KEY ((" in t for t in tabelas)  # chave de partição composta com o dia
