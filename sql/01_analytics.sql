-- Estrutura do banco de destino. Roda uma vez, na subida do container.
-- Todos os comandos são idempotentes.

\connect analytics

CREATE SCHEMA IF NOT EXISTS clima;

-- ---------------------------------------------------------------------------
-- Medições validas
-- ---------------------------------------------------------------------------
-- A chave natural (estação + instante) é única. É ela que torna a carga
-- idempotente: reprocessar um dia atualiza as mesmas linhas em vez de
-- duplicar, o que importa muito num DAG com retry automático.
CREATE TABLE IF NOT EXISTS clima.medicao (
    id              bigserial PRIMARY KEY,
    estacao_id      text        NOT NULL,
    cidade          text        NOT NULL,
    uf              char(2)     NOT NULL,
    medido_em       timestamptz NOT NULL,
    dia             date        NOT NULL,
    temperatura_c   numeric(5,1) NOT NULL,
    umidade_pct     numeric(5,1) NOT NULL,
    pressao_hpa     numeric(6,1) NOT NULL,
    vento_kmh       numeric(5,1) NOT NULL,
    precipitacao_mm numeric(5,1) NOT NULL,
    carregado_em    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (estacao_id, medido_em)
);

CREATE INDEX IF NOT EXISTS idx_medicao_dia ON clima.medicao (dia);
CREATE INDEX IF NOT EXISTS idx_medicao_estacao_dia ON clima.medicao (estacao_id, dia);

-- ---------------------------------------------------------------------------
-- Quarentena
-- ---------------------------------------------------------------------------
-- Leitura inválida não pode derrubar a ingestão do dia inteiro, e nem pode
-- sumir. Guardar o registro cru em JSONB permite reprocessar depois de
-- corrigir a regra, sem voltar a origem.
CREATE TABLE IF NOT EXISTS clima.medicao_rejeitada (
    id          bigserial PRIMARY KEY,
    dia         date        NOT NULL,
    motivo      text        NOT NULL,
    registro    jsonb       NOT NULL,
    criado_em   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rejeitada_dia ON clima.medicao_rejeitada (dia);

-- ---------------------------------------------------------------------------
-- Agregado diário
-- ---------------------------------------------------------------------------
-- Recalculado a cada execução a partir da tabela de medições. Manter o
-- agregado derivado, e não incrementado, evita o pior erro de consolidação:
-- somar duas vezes o mesmo dia depois de um reprocessamento.
CREATE TABLE IF NOT EXISTS clima.resumo_diario (
    estacao_id       text NOT NULL,
    dia              date NOT NULL,
    cidade           text NOT NULL,
    uf               char(2) NOT NULL,
    leituras         integer NOT NULL,
    temperatura_min  numeric(5,1) NOT NULL,
    temperatura_max  numeric(5,1) NOT NULL,
    temperatura_media numeric(5,1) NOT NULL,
    umidade_media    numeric(5,1) NOT NULL,
    precipitacao_total numeric(6,1) NOT NULL,
    vento_max        numeric(5,1) NOT NULL,
    atualizado_em    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (estacao_id, dia)
);

-- ---------------------------------------------------------------------------
-- Controle da pipeline
-- ---------------------------------------------------------------------------
-- O Airflow já guarda o histórico das execuções, mas em metadados próprios.
-- Esta tabela fica junto do dado, onde o time de análise consegue consultar
-- sem acesso ao Airflow -- e sobrevive a uma limpeza do banco de metadados.
CREATE TABLE IF NOT EXISTS clima.execucao (
    id            bigserial PRIMARY KEY,
    dag_id        text        NOT NULL,
    run_id        text        NOT NULL,
    dia           date        NOT NULL,
    lidas         integer     NOT NULL DEFAULT 0,
    validas       integer     NOT NULL DEFAULT 0,
    rejeitadas    integer     NOT NULL DEFAULT 0,
    taxa_rejeicao numeric(5,2) NOT NULL DEFAULT 0,
    situacao      text        NOT NULL,
    registrado_em timestamptz NOT NULL DEFAULT now(),
    UNIQUE (dag_id, run_id)
);

-- Somar todas as execuções de um dia contaria em dobro cada
-- reprocessamento. O DISTINCT ON pega apenas a execução mais recente de
-- cada dia, que é a que descreve o estado atual do dado.
CREATE OR REPLACE VIEW clima.vw_qualidade_diaria AS
SELECT DISTINCT ON (dia)
       dia,
       run_id,
       lidas,
       validas,
       rejeitadas,
       taxa_rejeicao AS taxa_rejeicao_pct,
       situacao,
       registrado_em
FROM clima.execucao
ORDER BY dia DESC, registrado_em DESC;
