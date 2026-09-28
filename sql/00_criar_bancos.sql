-- O container sobe com o banco de metadados do Airflow (POSTGRES_DB).
-- O banco analítico, que é o destino da pipeline, e criado aqui.
--
-- Separar os dois é proposital: limpar os metadados do Airflow em uma
-- atualização não pode levar junto o dado que a pipeline carregou.

CREATE DATABASE analytics;
