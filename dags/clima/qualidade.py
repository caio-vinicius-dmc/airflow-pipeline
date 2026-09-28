"""Regras de qualidade aplicadas a cada leitura.

A regra é sempre a mesma: nenhuma leitura ruim derruba a ingestão do dia.
Cada registro sai por um de dois caminhos -- vira medição válida ou vai
para a quarentena com o motivo anotado.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

# Faixas fisicamente plausíveis para as estações brasileiras cobertas.
# Valores fora daqui não são "outliers interessantes": são sensor com
# defeito ou sentinela do fabricante (o clássico -999).
FAIXAS = {
    "temperatura_c": (-15.0, 55.0),
    "umidade_pct": (0.0, 100.0),
    "pressao_hpa": (850.0, 1085.0),
    "vento_kmh": (0.0, 200.0),
    "precipitacao_mm": (0.0, 400.0),
}

OBRIGATORIOS = ("estacao_id", "cidade", "uf", "medido_em")


@dataclass(frozen=True)
class Rejeicao:
    motivo: str
    registro: dict


def _numero(valor) -> float | None:
    if valor is None:
        return None
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def validar(registro: dict) -> tuple[dict | None, Rejeicao | None]:
    """Devolve (medicao_valida, None) ou (None, rejeição)."""

    for campo in OBRIGATORIOS:
        valor = registro.get(campo)
        if valor is None or (isinstance(valor, str) and not valor.strip()):
            return None, Rejeicao(f"campo obrigatório ausente: {campo}", registro)

    try:
        medido_em = datetime.fromisoformat(registro["medido_em"])
    except (TypeError, ValueError):
        return None, Rejeicao(
            f"medido_em fora do formato ISO: {registro.get('medido_em')!r}", registro
        )

    limpo = {
        "estacao_id": registro["estacao_id"].strip(),
        "cidade": registro["cidade"].strip(),
        "uf": registro["uf"].strip().upper()[:2],
        "medido_em": medido_em,
        "dia": medido_em.date(),
    }

    for campo, (minimo, maximo) in FAIXAS.items():
        valor = _numero(registro.get(campo))
        if valor is None:
            return None, Rejeicao(f"{campo} ausente ou não numérico", registro)
        if not (minimo <= valor <= maximo):
            return None, Rejeicao(
                f"{campo} fora da faixa plausível: {valor} "
                f"(esperado entre {minimo} e {maximo})",
                registro,
            )
        limpo[campo] = valor

    return limpo, None


def separar(registros: list[dict]) -> tuple[list[dict], list[Rejeicao]]:
    validas: list[dict] = []
    rejeicoes: list[Rejeicao] = []

    for registro in registros:
        valida, rejeicao = validar(registro)
        if valida is not None:
            validas.append(valida)
        else:
            rejeicoes.append(rejeicao)

    return validas, rejeicoes


def remover_duplicatas(medicoes: list[dict]) -> tuple[list[dict], int]:
    """Mantém a última ocorrência de cada (estação, instante).

    Sem isso, o `ON CONFLICT DO UPDATE` da carga falha com
    "cannot affect row a second time" quando a origem reenvia uma leitura
    corrigida no mesmo lote.
    """
    por_chave: dict[tuple, dict] = {}
    for medicao in medicoes:
        por_chave[(medicao["estacao_id"], medicao["medido_em"])] = medicao
    return list(por_chave.values()), len(medicoes) - len(por_chave)


def resumir(lidas: int, validas: int, rejeitadas: int) -> dict:
    return {
        "lidas": lidas,
        "validas": validas,
        "rejeitadas": rejeitadas,
        "taxa_rejeicao": round(rejeitadas / lidas * 100, 2) if lidas else 0.0,
    }
