"""DAG de ingestão diária das medições meteorológicas.

Fluxo:

    aguardar_api -> extrair -> validar -> decidir_carga -> carregar -> consolidar
                                                        \\-> interromper_carga
                                                                           |
                                                              registrar_execucao

A decisão no meio existe porque nem toda origem ruim merece o mesmo
tratamento: 4% de leituras rejeitadas é o normal desta API e a carga segue;
acima de 20% quase sempre significa que o sensor ou o formato mudou, e aí
carregar é pior do que não carregar.
"""

from __future__ import annotations

import logging
import pendulum
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import Param, dag, task

from clima import api, qualidade, repositorio

logger = logging.getLogger(__name__)

# Acima deste percentual de rejeição a carga é bloqueada. Fica como
# parâmetro do DAG para poder ser afrouxado numa execução pontual sem
# alterar o código.
LIMITE_REJEICAO_PADRAO = 20.0


@dag(
    dag_id="ingestao_clima",
    description="Ingestão diária de medições meteorológicas com validação e quarentena",
    # Todo dia às 7h. O horário é interpretado no fuso do `start_date`, ou
    # seja, 7h de Brasília -- e não 7h UTC, que daria 4h da manhã aqui.
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    # catchup=False evita que a primeira subida dispare centenas de execuções
    # retroativas. Para carregar o histórico, use o backfill explicitamente --
    # está documentado em docs/operacao.md.
    catchup=False,
    max_active_runs=1,
    tags=["clima", "ingestao", "qualidade"],
    default_args={
        "owner": "dados",
        "retries": 3,
        # Espera crescente entre tentativas: 1min, 2min, 4min. Origem
        # instável costuma se recuperar sozinha, e insistir de imediato só
        # piora a situação dela.
        "retry_delay": pendulum.duration(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=10),
    },
    params={
        "limite_rejeicao_pct": Param(
            LIMITE_REJEICAO_PADRAO,
            type="number",
            description="Percentual de rejeição acima do qual a carga é bloqueada",
        ),
    },
    doc_md=__doc__,
)
def ingestao_clima():

    @task(retries=5, retry_delay=pendulum.duration(seconds=20))
    def aguardar_api() -> dict:
        """Confere que a origem está no ar antes de tentar extrair.

        Faz o papel de um sensor. Separar essa verificação da extração
        deixa claro, no log e na interface, se o problema foi a origem
        estar fora do ar ou o dado dela estar ruim.
        """
        return api.esperar_disponivel()

    @task
    def extrair(**contexto) -> dict:
        """Busca todas as páginas do dia da execução.

        `data_interval_start` é a data que a execução representa, e não a
        data de hoje. É o que faz o reprocessamento de uma data antiga
        buscar o dado daquela data.
        """
        dia = contexto["data_interval_start"].format("YYYY-MM-DD")

        try:
            registros = api.buscar_medicoes(dia)
        except api.SemDadosParaOData as erro:
            # Ausência de dado não é falha da pipeline. Marcar como pulada
            # evita encher a tela de vermelho por um fim de semana sem
            # coleta -- e o retry não resolveria mesmo.
            raise AirflowSkipException(str(erro)) from erro

        logger.info("Extraídos %d registros de %s", len(registros), dia)
        return {"dia": dia, "registros": registros}

    @task
    def validar(extraido: dict) -> dict:
        """Aplica as regras de qualidade e separa o que não passa."""
        dia = extraido["dia"]
        registros = extraido["registros"]

        validas, rejeicoes = qualidade.separar(registros)
        validas, duplicadas = qualidade.remover_duplicatas(validas)

        metricas = qualidade.resumir(
            lidas=len(registros), validas=len(validas), rejeitadas=len(rejeicoes)
        )
        metricas["duplicadas"] = duplicadas

        logger.info(
            "%s: %d lidas, %d válidas, %d rejeitadas (%.2f%%), %d duplicadas",
            dia, metricas["lidas"], metricas["validas"],
            metricas["rejeitadas"], metricas["taxa_rejeicao"], duplicadas,
        )

        motivos: dict[str, int] = {}
        for rejeicao in rejeicoes:
            motivos[rejeicao.motivo] = motivos.get(rejeicao.motivo, 0) + 1
        for motivo, quantidade in sorted(motivos.items(), key=lambda x: -x[1]):
            logger.info("  rejeição: %s (%d)", motivo, quantidade)

        # As medições válidas saem daqui em XComs. Para este volume (menos
        # de 200 registros por dia) é adequado; com milhões de linhas o
        # caminho seria gravar em staging e passar só o ponteiro.
        return {
            "dia": dia,
            "metricas": metricas,
            "medicoes": [
                {**m, "medido_em": m["medido_em"].isoformat(), "dia": m["dia"].isoformat()}
                for m in validas
            ],
            "rejeicoes": [
                {"motivo": r.motivo, "registro": r.registro} for r in rejeicoes
            ],
        }

    @task.branch
    def decidir_carga(validado: dict, **contexto) -> str:
        """Escolhe entre carregar e interromper, conforme a qualidade."""
        limite = float(contexto["params"]["limite_rejeicao_pct"])
        taxa = validado["metricas"]["taxa_rejeicao"]

        if taxa > limite:
            logger.error(
                "Taxa de rejeição de %.2f%% acima do limite de %.2f%%: carga bloqueada.",
                taxa, limite,
            )
            return "interromper_carga"

        logger.info("Taxa de rejeição de %.2f%% dentro do limite.", taxa)
        return "carregar"

    @task
    def carregar(validado: dict) -> dict:
        """Grava as medições válidas e a quarentena."""
        dia = pendulum.parse(validado["dia"]).date()

        medicoes = [
            {
                **m,
                "medido_em": pendulum.parse(m["medido_em"]),
                "dia": pendulum.parse(m["dia"]).date(),
            }
            for m in validado["medicoes"]
        ]

        gravadas = repositorio.gravar_medicoes(medicoes)
        quarentena = repositorio.gravar_rejeitadas(dia, validado["rejeicoes"])

        logger.info("%s: %d medições gravadas, %d em quarentena", dia, gravadas, quarentena)
        return {"dia": validado["dia"], "gravadas": gravadas, "quarentena": quarentena}

    @task
    def consolidar(carga: dict) -> dict:
        """Recalcula o agregado diário a partir do que foi gravado."""
        dia = pendulum.parse(carga["dia"]).date()
        linhas = repositorio.consolidar_dia(dia)
        contagens = repositorio.contar_do_dia(dia)

        logger.info(
            "%s: %d estações consolidadas (%d medições no total)",
            dia, linhas, contagens["medicoes"],
        )
        return {"dia": carga["dia"], "estacoes": linhas, **contagens}

    @task
    def interromper_carga(validado: dict) -> None:
        """Falha a execução de propósito, com o motivo no log.

        Uma tarefa que falha é melhor do que uma que só avisa: o dia fica
        vermelho na interface, entra nas métricas de SLA e alguém precisa
        decidir o que fazer.
        """
        metricas = validado["metricas"]
        raise AirflowFailException(
            f"Carga de {validado['dia']} bloqueada: "
            f"{metricas['rejeitadas']} de {metricas['lidas']} leituras rejeitadas "
            f"({metricas['taxa_rejeicao']:.2f}%). "
            "Verifique a origem antes de liberar o reprocessamento."
        )

    @task(trigger_rule="none_failed_min_one_success")
    def registrar_execucao(validado: dict, **contexto) -> None:
        """Registra o resultado na tabela de controle, junto do dado.

        `none_failed_min_one_success` faz esta tarefa rodar tanto no caminho
        feliz quanto quando a carga foi apenas pulada pelo desvio -- mas não
        quando algo realmente falhou antes.
        """
        execucao = contexto["dag_run"]
        dia = pendulum.parse(validado["dia"]).date()
        contagens = repositorio.contar_do_dia(dia)
        situacao = "carregado" if contagens["medicoes"] else "sem_carga"

        repositorio.registrar_execucao(
            dag_id=execucao.dag_id,
            run_id=execucao.run_id,
            dia=dia,
            metricas=validado["metricas"],
            situacao=situacao,
        )
        logger.info("Execução registrada como '%s' para %s", situacao, dia)

    saude = aguardar_api()
    extraido = extrair()
    validado = validar(extraido)
    decisao = decidir_carga(validado)

    carga = carregar(validado)
    consolidado = consolidar(carga)
    bloqueio = interromper_carga(validado)
    registro = registrar_execucao(validado)

    saude >> extraido
    decisao >> [carga, bloqueio]
    consolidado >> registro


ingestao_clima()
