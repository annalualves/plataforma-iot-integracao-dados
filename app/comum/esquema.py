"""Contrato (schema) das mensagens de leitura de sensor no tópico sensores.leituras."""
from __future__ import annotations

from datetime import datetime

CAMPOS_OBRIGATORIOS = {
    "versao_schema": int, "evento_id": str, "sensor_id": str, "maquina_id": str,
    "linha_id": str, "setor": str, "tipo": str, "valor": float, "unidade": str,
    "status": str, "ts": str,
}


def validar_leitura(msg: dict) -> list[str]:
    """Devolve a lista de problemas da mensagem (vazia se estiver válida)."""
    problemas = []
    for campo, tipo in CAMPOS_OBRIGATORIOS.items():
        if campo not in msg:
            problemas.append(f"campo ausente: {campo}")
        elif tipo is float and not isinstance(msg[campo], (int, float)):
            problemas.append(f"tipo inválido em {campo}")
        elif tipo is not float and not isinstance(msg[campo], tipo):
            problemas.append(f"tipo inválido em {campo}")
    if msg.get("status") not in ("ok", "warning", "error"):
        problemas.append("status inválido")
    try:
        datetime.fromisoformat(str(msg.get("ts", "")).replace("Z", "+00:00"))
    except ValueError:
        problemas.append("ts não é ISO 8601")
    return problemas
