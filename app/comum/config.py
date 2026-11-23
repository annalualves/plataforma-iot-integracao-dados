"""Configuração lida das variáveis de ambiente (definidas no docker-compose.yml)."""
import os
from pathlib import Path


def _env(nome: str, padrao: str) -> str:
    return os.environ.get(nome, padrao)


KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:29092")
TOPICO_LEITURAS = _env("TOPICO_LEITURAS", "sensores.leituras")

S3_ENDPOINT = _env("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = _env("S3_ACCESS_KEY", "rustfsadmin")
S3_SECRET_KEY = _env("S3_SECRET_KEY", "rustfsadmin123")
BUCKET_BRONZE = _env("BUCKET_BRONZE", "bronze")
BUCKETS = [BUCKET_BRONZE, "silver", "gold"]

CASSANDRA_HOST = _env("CASSANDRA_HOST", "localhost")
CASSANDRA_KEYSPACE = _env("CASSANDRA_KEYSPACE", "iot")

REDIS_HOST = _env("REDIS_HOST", "localhost")

MONGO_URI = _env("MONGO_URI", "mongodb://admin:admin123@localhost:27017/?authSource=admin")
MONGO_DB = _env("MONGO_DB", "fabrica")

NEO4J_URI = _env("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = _env("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = _env("NEO4J_PASSWORD", "senha12345")

LOTE_MAX_MENSAGENS = int(_env("LOTE_MAX_MENSAGENS", "2000"))
LOTE_MAX_SEGUNDOS = float(_env("LOTE_MAX_SEGUNDOS", "60"))

CATALOGO_PATH = Path(__file__).resolve().parent.parent / "catalogo" / "fabrica.json"
