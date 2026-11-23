"""Geração das leituras dos sensores (sem Kafka, para poder ser testada isoladamente).

Cada sensor segue um passeio aleatório que tende a voltar para a sua média.
Com probabilidade `prob_anomalia` por leitura, começa uma anomalia:

- pico:     uma leitura isolada muito acima do normal
- travado:  o sensor repete o mesmo valor por 10 a 30 leituras
- ausente:  o sensor para de enviar por 10 a 30 leituras
- deriva:   o valor sobe aos poucos por 20 a 60 leituras (desgaste, superaquecimento)

O campo `anomalia_injetada` registra o tipo da anomalia (ou null). Ele é o
"gabarito" para avaliar, na Entrega 3, se a detecção de anomalias funciona.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timezone

TIPOS_ANOMALIA = ("pico", "travado", "ausente", "deriva")
VERSAO_SCHEMA = 1


@dataclass
class EstadoSensor:
    valor: float
    anomalia: str | None = None
    restantes: int = 0
    passo_deriva: float = 0.0
    seq: int = 0
    extra: dict = field(default_factory=dict)


def classificar_status(valor: float, limites: dict) -> str:
    if valor >= limites["error"]:
        return "error"
    if valor >= limites["warning"]:
        return "warning"
    return "ok"


class GeradorLeituras:
    def __init__(self, sensores: list[dict], seed: int = 42, prob_anomalia: float = 0.01):
        if not 0.0 <= prob_anomalia <= 1.0:
            raise ValueError("prob_anomalia deve estar entre 0 e 1")
        self.sensores = sensores
        self.rng = random.Random(seed)
        self.prob_anomalia = prob_anomalia
        self.estado = {s["id"]: EstadoSensor(valor=s["media"]) for s in sensores}

    def _iniciar_anomalia(self, sensor: dict, est: EstadoSensor) -> None:
        est.anomalia = self.rng.choice(TIPOS_ANOMALIA)
        if est.anomalia == "pico":
            est.restantes = 1
        elif est.anomalia in ("travado", "ausente"):
            est.restantes = self.rng.randint(10, 30)
        else:  # deriva
            est.restantes = self.rng.randint(20, 60)
            alvo = sensor["limites"]["error"] * 1.05
            est.passo_deriva = (alvo - est.valor) / est.restantes

    def _valor_normal(self, sensor: dict, est: EstadoSensor) -> float:
        # passeio aleatório com reversão à média
        return est.valor + 0.2 * (sensor["media"] - est.valor) + self.rng.gauss(0, sensor["desvio"] * 0.3)

    def leitura(self, sensor: dict, agora: datetime) -> dict | None:
        """Gera a próxima leitura do sensor. Retorna None quando o sensor está 'ausente'."""
        est = self.estado[sensor["id"]]
        est.seq += 1

        if est.anomalia is None and self.rng.random() < self.prob_anomalia:
            self._iniciar_anomalia(sensor, est)

        anomalia = est.anomalia
        if anomalia == "pico":
            valor = sensor["limites"]["error"] * self.rng.uniform(1.05, 1.3)
        elif anomalia == "travado":
            valor = est.valor  # repete o último valor
        elif anomalia == "ausente":
            valor = None
        elif anomalia == "deriva":
            valor = est.valor + est.passo_deriva
        else:
            valor = self._valor_normal(sensor, est)

        if anomalia is not None:
            est.restantes -= 1
            if est.restantes <= 0:
                est.anomalia = None

        if valor is None:
            return None

        if anomalia != "pico":  # o pico não altera o "estado" do sensor
            est.valor = valor

        valor = round(valor, 3)
        ts = agora.astimezone(timezone.utc)
        return {
            "versao_schema": VERSAO_SCHEMA,
            "evento_id": f"{sensor['id']}:{est.seq}",
            "sensor_id": sensor["id"],
            "maquina_id": sensor["maquina_id"],
            "linha_id": sensor["linha_id"],
            "setor": sensor["setor"],
            "tipo": sensor["tipo"],
            "valor": valor,
            "unidade": sensor["unidade"],
            "status": classificar_status(valor, sensor["limites"]),
            "ts": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "anomalia_injetada": anomalia,
        }

    def rodada(self, agora: datetime) -> list[dict]:
        """Uma leitura de cada sensor no instante `agora` (sensores ausentes ficam de fora)."""
        leituras = (self.leitura(s, agora) for s in self.sensores)
        return [l for l in leituras if l is not None]
