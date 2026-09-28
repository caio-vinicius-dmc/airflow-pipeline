"""Cliente da API de medições.

Fica separado do DAG de propósito: a conversa com o sistema de origem muda
por motivos diferentes dos que fazem a orquestração mudar, e um módulo
próprio pode ser exercitado fora do Airflow.
"""

from __future__ import annotations

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

URL_BASE = os.getenv("API_CLIMA_URL", "http://api-clima:8000")

# Tempo limite sempre explícito. Sem ele, uma origem que trava deixa a
# tarefa pendurada até o timeout do Airflow -- e o retry nunca acontece.
TEMPO_LIMITE = 15


class ApiIndisponivel(Exception):
    """A origem não respondeu, ou respondeu com erro de servidor."""


class SemDadosParaOData(Exception):
    """A origem não tem dados para a data pedida (404).

    É diferente de indisponibilidade: repetir não vai resolver.
    """


def esperar_disponivel(tentativas: int = 12, intervalo: float = 5.0) -> dict:
    """Aguarda a API responder, em vez de falhar no primeiro erro.

    Faz o papel de um sensor. Num `docker compose up`, a API e o Airflow
    sobem juntos e a primeira execução costuma chegar antes da origem
    estar pronta.
    """
    ultimo_erro: Exception | None = None

    for tentativa in range(1, tentativas + 1):
        try:
            resposta = requests.get(f"{URL_BASE}/saude", timeout=TEMPO_LIMITE)
            resposta.raise_for_status()
            corpo = resposta.json()
            logger.info("API respondeu na tentativa %d: %s", tentativa, corpo)
            return corpo
        except requests.RequestException as erro:
            ultimo_erro = erro
            logger.warning(
                "API ainda não respondeu (tentativa %d/%d): %s",
                tentativa, tentativas, erro,
            )
            time.sleep(intervalo)

    raise ApiIndisponivel(
        f"A API em {URL_BASE} não respondeu após {tentativas} tentativas. "
        f"Último erro: {ultimo_erro}"
    )


def buscar_medicoes(dia: str) -> list[dict]:
    """Busca todas as páginas de um dia.

    A paginação é resolvida aqui dentro. Espalhar o laço de páginas pelo
    DAG transformaria um detalhe da origem em estrutura de orquestração --
    e quebraria o DAG inteiro se a API mudasse o tamanho da página.
    """
    itens: list[dict] = []
    pagina = 1
    total_paginas = 1

    while pagina <= total_paginas:
        try:
            resposta = requests.get(
                f"{URL_BASE}/medicoes",
                params={"data": dia, "pagina": pagina},
                timeout=TEMPO_LIMITE,
            )
        except requests.RequestException as erro:
            raise ApiIndisponivel(f"Falha ao chamar a API: {erro}") from erro

        if resposta.status_code == 404:
            raise SemDadosParaOData(
                f"A origem não tem medições para {dia}: {resposta.text}"
            )
        if resposta.status_code >= 500:
            raise ApiIndisponivel(
                f"A origem devolveu {resposta.status_code} para {dia}"
            )
        resposta.raise_for_status()

        corpo = resposta.json()
        total_paginas = corpo["total_paginas"]
        itens.extend(corpo["itens"])

        logger.info(
            "Página %d/%d de %s: %d registros",
            pagina, total_paginas, dia, len(corpo["itens"]),
        )
        pagina += 1

    return itens
