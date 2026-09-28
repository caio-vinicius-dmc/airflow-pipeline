"""API de medições meteorológicas.

Este serviço existe para a pipeline ter uma origem HTTP de verdade para
consumir: paginação, código de status, registro sujo. Sem ele o DAG estaria
lendo um arquivo local, e o trecho mais frágil de qualquer ingestão -- a
conversa com o sistema de origem -- ficaria de fora.

Usa só a biblioteca padrão, então roda em qualquer imagem Python sem
instalar nada.

Rotas:
    GET /saude
    GET /medicoes?data=AAAA-MM-DD&página=1
"""

from __future__ import annotations

import json
import math
import os
import random
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# Cada estação tem um deslocamento próprio de temperatura e uma amplitude
# sazonal própria. Derivar a temperatura da posição na lista, como a
# primeira versão fazia, deixava Curitiba mais quente que São Paulo.
#
#            id         cidade            uf   offset  amplitude
ESTACOES = [
    ("EST-001", "Sao Paulo",      "SP",  0.0,   1.00),
    ("EST-002", "Campinas",       "SP",  1.2,   1.05),
    ("EST-003", "Rio de Janeiro", "RJ",  3.5,   0.75),
    ("EST-004", "Belo Horizonte", "MG",  1.8,   0.85),
    ("EST-005", "Curitiba",       "PR", -3.2,   1.15),
    ("EST-006", "Porto Alegre",   "RS", -1.4,   1.35),
    ("EST-007", "Salvador",       "BA",  6.0,   0.40),
    # Manaus é equatorial: quente o ano inteiro, quase sem estação.
    ("EST-008", "Manaus",         "AM",  7.2,   0.20),
]

# Uma leitura por estação a cada hora.
LEITURAS_POR_ESTACAO = 24
TAMANHO_PAGINA = 60

# Fração de leituras que saem com defeito. Sem elas a etapa de validação da
# pipeline nunca teria o que rejeitar e o gráfico de qualidade seria uma
# linha reta em 100%.
TAXA_DEFEITO = 0.04

PRIMEIRA_DATA = date(2025, 1, 1)


def _semente(dia: date) -> int:
    """Semente derivada da data: a mesma data devolve sempre os mesmos dados.

    É o que permite reprocessar uma janela e conferir que o resultado bate
    com o da primeira execução.
    """
    return dia.toordinal()


def _defeito(aleatorio: random.Random, leitura: dict) -> dict:
    tipo = aleatorio.choice(["temperatura", "umidade", "nulo", "timestamp", "estacao"])
    quebrada = dict(leitura)
    if tipo == "temperatura":
        quebrada["temperatura_c"] = -999.0        # sentinela de sensor offline
    elif tipo == "umidade":
        quebrada["umidade_pct"] = 148.0           # fora da faixa física
    elif tipo == "nulo":
        quebrada["pressao_hpa"] = None
    elif tipo == "timestamp":
        quebrada["medido_em"] = "ontem as 14h"
    else:
        quebrada["estacao_id"] = ""
    return quebrada


def gerar_medicoes(dia: date) -> list[dict]:
    aleatorio = random.Random(_semente(dia))
    medicoes: list[dict] = []

    # Amplitude térmica ao longo do ano, para a série não ficar plana.
    # O pico fica em janeiro e o vale em julho: hemisfério sul. Com o sinal
    # invertido, junho saía mais quente que o verão.
    dia_do_ano = dia.timetuple().tm_yday
    sazonal = 6.5 * math.cos((dia_do_ano - 20) / 365 * 2 * math.pi)

    # Dia seco ou chuvoso, sorteado uma vez para o dia inteiro. Sem isso
    # chovia todo dia em todas as estações, e o total diário de chuva ficava
    # em torno de 25 mm sempre -- o que nenhum lugar do país faz.
    intensidade_chuva = 0.0 if aleatorio.random() < 0.58 else aleatorio.uniform(0.4, 4.5)

    for estacao, cidade, uf, offset, amplitude in ESTACOES:
        base = 18 + offset + sazonal * amplitude
        for hora in range(LEITURAS_POR_ESTACAO):
            # Ciclo diário: mínima de madrugada, máxima no meio da tarde.
            ciclo = -5.5 * math.cos((hora - 15) / 24 * 2 * math.pi)
            leitura = {
                "estacao_id": estacao,
                "cidade": cidade,
                "uf": uf,
                "medido_em": datetime(
                    dia.year, dia.month, dia.day, hora, tzinfo=timezone.utc
                ).isoformat(),
                "temperatura_c": round(base + ciclo + aleatorio.uniform(-1.5, 1.5), 1),
                "umidade_pct": round(aleatorio.uniform(38, 92), 1),
                "pressao_hpa": round(aleatorio.uniform(1002, 1024), 1),
                "vento_kmh": round(aleatorio.uniform(0, 34), 1),
                "precipitacao_mm": round(
                    max(0.0, aleatorio.gauss(intensidade_chuva * 0.18, intensidade_chuva * 0.7)), 1
                ),
            }
            if aleatorio.random() < TAXA_DEFEITO:
                leitura = _defeito(aleatorio, leitura)
            medicoes.append(leitura)

    return medicoes


class Manipulador(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _responder(self, codigo: int, corpo: dict) -> None:
        conteudo = json.dumps(corpo, ensure_ascii=False).encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(conteudo)))
        self.end_headers()
        self.wfile.write(conteudo)

    def do_GET(self) -> None:  # noqa: N802  (assinatura exigida pela stdlib)
        rota = urlparse(self.path)
        parametros = parse_qs(rota.query)

        if rota.path == "/saude":
            self._responder(200, {"status": "ok", "estacoes": len(ESTACOES)})
            return

        if rota.path != "/medicoes":
            self._responder(404, {"erro": f"rota desconhecida: {rota.path}"})
            return

        bruto = parametros.get("data", [None])[0]
        if not bruto:
            self._responder(400, {"erro": "parâmetro 'data' e obrigatório"})
            return

        try:
            dia = date.fromisoformat(bruto)
        except ValueError:
            self._responder(400, {"erro": f"data inválida: {bruto}"})
            return

        # Data futura devolve 404, como faria uma API de verdade. É o que
        # obriga a pipeline a tratar o caso em vez de gravar dia vazio.
        if dia > date.today():
            self._responder(404, {"erro": f"ainda não há medições para {dia}"})
            return
        if dia < PRIMEIRA_DATA:
            self._responder(
                404, {"erro": f"o histórico começa em {PRIMEIRA_DATA.isoformat()}"}
            )
            return

        try:
            pagina = max(1, int(parametros.get("pagina", ["1"])[0]))
        except ValueError:
            self._responder(400, {"erro": "página precisa ser um inteiro"})
            return

        todas = gerar_medicoes(dia)
        total_paginas = max(1, -(-len(todas) // TAMANHO_PAGINA))
        inicio = (pagina - 1) * TAMANHO_PAGINA
        itens = todas[inicio : inicio + TAMANHO_PAGINA]

        self._responder(
            200,
            {
                "data": dia.isoformat(),
                "pagina": pagina,
                "total_paginas": total_paginas,
                "total_itens": len(todas),
                "itens": itens,
            },
        )

    def log_message(self, formato: str, *args) -> None:
        # Log enxuto: o padrão da stdlib escreve em stderr num formato que
        # polui a saída do compose.
        print(f"[api-clima] {self.address_string()} {formato % args}", flush=True)


def main() -> None:
    porta = int(os.getenv("PORTA", "8000"))
    servidor = ThreadingHTTPServer(("0.0.0.0", porta), Manipulador)
    print(f"[api-clima] ouvindo na porta {porta}", flush=True)
    servidor.serve_forever()


if __name__ == "__main__":
    main()
