"""Acesso ao banco analítico.

A conexão vem de uma Connection do Airflow (`postgres_analytics`), criada
na subida do ambiente. Nenhuma credencial aparece no código nem no DAG: o
que o repositório conhece é o nome da conexão.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import date
from typing import Iterator

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

CONEXAO = "postgres_analytics"


@contextmanager
def conectar() -> Iterator:
    gancho = PostgresHook(postgres_conn_id=CONEXAO)
    conexao = gancho.get_conn()
    try:
        yield conexao
        conexao.commit()
    except Exception:
        conexao.rollback()
        raise
    finally:
        conexao.close()


def gravar_medicoes(medicoes: list[dict]) -> int:
    """Upsert pela chave natural (estação, instante).

    É o que torna a carga segura para o retry automático do Airflow:
    a segunda tentativa atualiza as mesmas linhas em vez de duplicar.
    """
    if not medicoes:
        return 0

    registros = [
        (
            m["estacao_id"], m["cidade"], m["uf"], m["medido_em"], m["dia"],
            m["temperatura_c"], m["umidade_pct"], m["pressao_hpa"],
            m["vento_kmh"], m["precipitacao_mm"],
        )
        for m in medicoes
    ]

    with conectar() as conexao:
        with conexao.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO clima.medicao (
                    estacao_id, cidade, uf, medido_em, dia,
                    temperatura_c, umidade_pct, pressao_hpa,
                    vento_kmh, precipitacao_mm
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (estacao_id, medido_em) DO UPDATE
                    SET cidade          = EXCLUDED.cidade,
                        uf              = EXCLUDED.uf,
                        dia             = EXCLUDED.dia,
                        temperatura_c   = EXCLUDED.temperatura_c,
                        umidade_pct     = EXCLUDED.umidade_pct,
                        pressao_hpa     = EXCLUDED.pressao_hpa,
                        vento_kmh       = EXCLUDED.vento_kmh,
                        precipitacao_mm = EXCLUDED.precipitacao_mm,
                        carregado_em    = now()
                """,
                registros,
            )

    return len(registros)


def gravar_rejeitadas(dia: date, rejeicoes: list) -> int:
    """Regrava a quarentena do dia.

    O DELETE antes do INSERT evita que um reprocessamento acumule as mesmas
    rejeições várias vezes e distorça o indicador de qualidade.
    """
    with conectar() as conexao:
        with conexao.cursor() as cursor:
            cursor.execute("DELETE FROM clima.medicao_rejeitada WHERE dia = %s", (dia,))
            if rejeicoes:
                cursor.executemany(
                    """
                    INSERT INTO clima.medicao_rejeitada (dia, motivo, registro)
                    VALUES (%s, %s, %s)
                    """,
                    [
                        (dia, r["motivo"], json.dumps(r["registro"], ensure_ascii=False))
                        for r in rejeicoes
                    ],
                )

    return len(rejeicoes)


def consolidar_dia(dia: date) -> int:
    """Recalcula o agregado do dia a partir das medições.

    Derivar em vez de incrementar é o que evita o erro clássico de
    consolidação: somar duas vezes o mesmo dia depois de um reprocessamento.
    """
    with conectar() as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO clima.resumo_diario (
                    estacao_id, dia, cidade, uf, leituras,
                    temperatura_min, temperatura_max, temperatura_media,
                    umidade_media, precipitacao_total, vento_max
                )
                SELECT estacao_id,
                       dia,
                       min(cidade),
                       min(uf),
                       count(*),
                       min(temperatura_c),
                       max(temperatura_c),
                       round(avg(temperatura_c), 1),
                       round(avg(umidade_pct), 1),
                       round(sum(precipitacao_mm), 1),
                       max(vento_kmh)
                  FROM clima.medicao
                 WHERE dia = %s
                 GROUP BY estacao_id, dia
                ON CONFLICT (estacao_id, dia) DO UPDATE
                    SET cidade             = EXCLUDED.cidade,
                        uf                 = EXCLUDED.uf,
                        leituras           = EXCLUDED.leituras,
                        temperatura_min    = EXCLUDED.temperatura_min,
                        temperatura_max    = EXCLUDED.temperatura_max,
                        temperatura_media  = EXCLUDED.temperatura_media,
                        umidade_media      = EXCLUDED.umidade_media,
                        precipitacao_total = EXCLUDED.precipitacao_total,
                        vento_max          = EXCLUDED.vento_max,
                        atualizado_em      = now()
                """,
                (dia,),
            )
            return cursor.rowcount


def registrar_execucao(
    dag_id: str, run_id: str, dia: date, metricas: dict, situacao: str
) -> None:
    with conectar() as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO clima.execucao (
                    dag_id, run_id, dia, lidas, validas, rejeitadas,
                    taxa_rejeicao, situacao
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dag_id, run_id) DO UPDATE
                    SET lidas         = EXCLUDED.lidas,
                        validas       = EXCLUDED.validas,
                        rejeitadas    = EXCLUDED.rejeitadas,
                        taxa_rejeicao = EXCLUDED.taxa_rejeicao,
                        situacao      = EXCLUDED.situacao,
                        registrado_em = now()
                """,
                (
                    dag_id, run_id, dia,
                    metricas["lidas"], metricas["validas"], metricas["rejeitadas"],
                    metricas["taxa_rejeicao"], situacao,
                ),
            )


def contar_do_dia(dia: date) -> dict:
    with conectar() as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM clima.medicao WHERE dia = %s", (dia,)
            )
            medicoes = cursor.fetchone()[0]
            cursor.execute(
                "SELECT count(*) FROM clima.resumo_diario WHERE dia = %s", (dia,)
            )
            resumos = cursor.fetchone()[0]
    return {"medicoes": medicoes, "resumos": resumos}
