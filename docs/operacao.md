# Operação

Como rodar, reprocessar e diagnosticar a pipeline no dia a dia.

## Executar um dia específico

```bash
docker compose exec airflow-scheduler airflow dags test ingestao_clima 2025-07-15
```

`dags test` roda o DAG inteiro no processo atual e imprime tudo no
terminal. É a forma mais rápida de ver o que aconteceu, porque os logs de
todas as tarefas saem juntos, em ordem.

A data passada é a **data lógica** da execução, e é ela que o DAG usa para
pedir os dados -- não a data de hoje. É o que faz o reprocessamento de uma
data antiga buscar o dado daquela data.

## Carregar o histórico (backfill)

O DAG sobe com `catchup=False` de propósito: sem isso, a primeira subida
dispararia uma execução para cada dia desde janeiro de 2025 e o ambiente
levaria muito tempo para responder.

Para carregar um período:

```bash
docker compose exec airflow-scheduler \
  airflow backfill create \
    --dag-id ingestao_clima \
    --from-date 2025-07-01 \
    --to-date 2025-07-31 \
    --max-active-runs 1
```

`--max-active-runs 1` importa. O DAG já tem `max_active_runs=1` na
definição, mas deixar explícito evita que um backfill grande abra dezenas
de execuções paralelas contra a origem.

Acompanhe pela interface, em **Browse > DAG Runs**.

## Reprocessar um dia já carregado

Simplesmente rode de novo. A carga é idempotente:

- as medições entram por upsert na chave (estação, instante);
- a quarentena do dia é apagada antes de ser regravada;
- o agregado diário é recalculado a partir das medições, não incrementado.

Rodar duas vezes o mesmo dia produz exatamente o mesmo estado final. Foi
verificado: a segunda execução de 15/07 manteve as mesmas 180 medições, sem
duplicar nenhuma linha.

## Quando a carga é bloqueada

A tarefa `interromper_carga` falha a execução quando a taxa de rejeição
passa do limite. O log traz o motivo:

```
Carga de 2025-06-17 bloqueada: 10 de 192 leituras rejeitadas (5.21%).
Verifique a origem antes de liberar o reprocessamento.
```

O que fazer:

1. Olhe os motivos de rejeição daquele dia:

   ```sql
   SELECT motivo, count(*)
     FROM clima.medicao_rejeitada
    WHERE dia = '2025-06-17'
    GROUP BY motivo ORDER BY 2 DESC;
   ```

   A quarentena é gravada **antes** do bloqueio, então o diagnóstico está
   disponível mesmo com a carga interrompida.

2. Se o problema for a origem, resolva lá e reprocesse normalmente.

3. Se o problema for a regra estar rígida demais para aquele dia, libere
   pontualmente:

   ```bash
   docker compose exec airflow-scheduler \
     airflow dags test ingestao_clima 2025-06-17 --conf '{"limite_rejeicao_pct": 30}'
   ```

   Liberar pelo parâmetro deixa rastro na execução. Alterar o código para
   passar um dia específico, não.

## Consultas de diagnóstico

```sql
-- Qualidade por dia (apenas a execução mais recente de cada um)
SELECT * FROM clima.vw_qualidade_diaria;

-- Dias sem carga no período
SELECT d::date AS dia
  FROM generate_series('2025-07-01'::date, '2025-07-31'::date, '1 day') AS d
 WHERE NOT EXISTS (
     SELECT 1 FROM clima.medicao m WHERE m.dia = d::date
 );

-- Estações que pararam de reportar
SELECT estacao_id, max(dia) AS ultimo_dia
  FROM clima.medicao
 GROUP BY estacao_id
 ORDER BY ultimo_dia;

-- Leituras esperadas x recebidas (8 estações x 24 horas = 192 por dia)
SELECT dia, count(*) AS recebidas, 192 - count(*) AS faltando
  FROM clima.medicao
 GROUP BY dia
 HAVING count(*) < 192
 ORDER BY dia DESC;
```

A última é a mais útil na prática. Um dia com 180 leituras em vez de 192
não é erro: são as 12 rejeitadas pela validação. Um dia com 96 significa
que metade das estações não reportou.

## Acessar os bancos

```bash
# banco analítico (destino da pipeline)
docker compose exec postgres psql -U airflow -d analytics

# banco de metadados do Airflow
docker compose exec postgres psql -U airflow -d airflow
```

De fora do container, a porta é a do `.env` (`POSTGRES_PORT`, 15435 por
padrão).

## Quando algo não sobe

**O DAG não aparece na interface.** Veja o log do leitor de DAGs:

```bash
docker compose logs airflow-dag-processor | grep -i error
```

Erro de import costuma ser `ModuleNotFoundError: No module named 'clima'`.
O Airflow 3 não adiciona mais a pasta de DAGs ao `sys.path` sozinho -- é o
`PYTHONPATH: /opt/airflow/dags` no compose que resolve isso. Se você mover
os módulos auxiliares para outro lugar, ajuste ali.

**`cryptography.fernet.InvalidToken` na inicialização.** A `AIRFLOW_FERNET_KEY`
mudou entre duas subidas, e as senhas das Connections gravadas antes não
podem mais ser lidas. Fixe a chave no `.env` e recrie o ambiente com
`docker compose down -v`.

**A API não responde.** A tarefa `aguardar_api` tenta doze vezes com cinco
segundos de intervalo antes de desistir. Se ainda assim falhar:

```bash
docker compose logs api-clima
curl http://localhost:18000/saude
```

**Uma execução fica presa em `running`.** Costuma ser um `dags test` que foi
interrompido no meio (fechar o terminal, por exemplo). O scheduler marca a
execução como falha depois de um tempo. Rodar de novo resolve, já que a
carga é idempotente.
