# airflow-pipeline

Uma rotina que roda sozinha todo dia de manhã: busca dados de uma API,
confere cada registro, guarda o que presta e avisa quando alguma coisa
está errada demais para continuar.

## Do que se trata, em linguagem simples

**Apache Airflow** é a ferramenta que a maioria das empresas usa para
agendar e coordenar rotinas de dados. Ele resolve as perguntas chatas:
"essa rotina rodou hoje?", "se falhar, tenta de novo quantas vezes?", "a
etapa B só pode começar depois que a A terminar", "preciso reprocessar o
mês passado inteiro".

Uma rotina no Airflow se chama **DAG**. É só um desenho de quais tarefas
existem e em que ordem elas acontecem.

Este projeto tem um DAG que, todo dia às 7h, busca as medições de estações
meteorológicas do dia anterior, confere cada leitura, guarda as boas,
separa as ruins e registra o resultado.

O ambiente inteiro sobe com um comando. Não depende de nenhum serviço
externo nem de conta em lugar nenhum.

## O desenho da rotina

```
aguardar API ─→ extrair ─→ conferir ─→ decidir ─┬─→ carregar ─→ consolidar ─┐
                                                └─→ interromper             │
                                                                            ↓
                                                              registrar execução
```

| Tarefa | O que faz |
|--------|-----------|
| aguardar API | confere se a origem está no ar, esperando cada vez mais entre as tentativas |
| extrair | busca todas as páginas do dia |
| conferir | aplica as regras de qualidade e separa as leituras ruins |
| decidir | escolhe o caminho conforme a proporção de leituras ruins |
| carregar | grava as medições boas e a quarentena |
| consolidar | recalcula o resumo do dia |
| interromper | falha de propósito quando a qualidade está ruim demais |
| registrar execução | anota o resultado junto do dado, não só nos bastidores |

## O que você precisa ter instalado

- **Docker Desktop** —
  [docker.com](https://www.docker.com/products/docker-desktop/)
- Cerca de **4 GB de memória** livres para o Docker. É o projeto mais
  pesado desta coleção: sobe seis containers.

Não precisa de Python instalado: tudo roda dentro dos containers.

## Como rodar

**1. Crie o arquivo de configuração.**

```bash
cp .env.example .env
```

**2. Gere os dois segredos.** Abra o `.env` e substitua os valores de
exemplo. Estes dois comandos geram valores seguros:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

O primeiro vai em `AIRFLOW_JWT_SECRET`, o segundo em
`AIRFLOW_FERNET_KEY`. Se você não tiver Python na máquina, qualquer texto
longo e aleatório serve para o primeiro; o segundo precisa ser gerado pelo
comando, porque tem formato específico.

**3. Suba tudo.**

```bash
docker compose up -d
```

A primeira subida leva um ou dois minutos, porque o Docker baixa as
imagens. Depois é rápido.

**4. Abra a interface** em <http://localhost:18080>. Você vai ver o DAG
`ingestao_clima` com as oito tarefas desenhadas.

**5. Rode um dia específico** sem esperar as 7h:

```bash
docker compose exec airflow-scheduler airflow dags test ingestao_clima 2025-07-15
```

### Quando terminar

```bash
docker compose down -v
```

## O que sai

```
2025-07-15: 180 medições gravadas, 12 em quarentena
2025-07-15: 8 estações consolidadas (180 medições no total)
Execução registrada como 'carregado' para 2025-07-15
```

Consultando o resultado no banco:

```sql
SELECT * FROM clima.vw_qualidade_diaria;

   dia      | lidas | validas | rejeitadas | taxa_rejeicao_pct | situacao
 2025-07-16 |   192 |     185 |          7 |              3.65 | sem_carga
 2025-07-15 |   192 |     180 |         12 |              6.25 | carregado
```

E os motivos das rejeições:

```
temperatura fora da faixa plausível: -999.0 (esperado entre -15.0 e 55.0)  | 8
data de medição fora do formato ISO: 'ontem as 14h'                        | 6
campo obrigatório ausente: estacao_id                                      | 6
pressão ausente ou não numérica                                            | 6
```

O `-999.0` não é invenção: é o valor que muitos sensores emitem quando
perdem a leitura. Sem a conferência, ele entraria como temperatura e
puxaria a média do dia para baixo sem ninguém perceber.

## O que este projeto demonstra

**Rodar de novo é seguro.** Processar o mesmo dia duas vezes não duplica
nada. A gravação identifica cada leitura pela estação e pelo instante, e o
resumo do dia é recalculado do zero em vez de somado. Isso importa numa
rotina que tenta de novo automaticamente.

**Nenhuma leitura ruim derruba o dia.** Registro inválido vai para a
quarentena com o motivo e o conteúdo original. Dá para reprocessar depois
de corrigir a regra, sem voltar à origem.

**A rotina desvia quando a qualidade cai.** Até 20% de rejeição, a carga
segue. Acima disso, a tarefa `interromper` falha de propósito. Quatro por
cento é o normal desta origem; vinte por cento quase sempre significa que
o formato mudou, e aí carregar é pior do que não carregar.

O limite é um parâmetro, então dá para afrouxar numa execução específica
sem mexer no código:

```bash
docker compose exec airflow-scheduler \
  airflow dags test ingestao_clima 2025-07-15 --conf '{"limite_rejeicao_pct": 1}'
```

**Falta de dado não é falha.** Quando a origem não tem dados para aquela
data, a tarefa se marca como "pulada" em vez de falhar. Um fim de semana
sem coleta não deve encher a tela de vermelho — e insistir não resolveria.

**Espera crescente entre tentativas.** Três tentativas, com o intervalo
dobrando: 1, 2 e 4 minutos. Origem instável costuma se recuperar sozinha,
e insistir de imediato só piora a situação dela.

## Estrutura das pastas

```
docker-compose.yml        o Airflow 3 com seis containers, versão enxuta
dags/ingestao_clima.py    a rotina em si
dags/clima/api.py         conversa com a origem, com paginação e tempo limite
dags/clima/qualidade.py   as regras de conferência
dags/clima/repositorio.py o acesso ao banco
servico-api/api.py        a API de origem, feita só com biblioteca padrão
sql/                      as tabelas do banco de destino
docs/operacao.md          reprocessamento, carga de histórico e diagnóstico
```

A lógica fica em módulos separados, não dentro do arquivo do DAG. O DAG
descreve **quando** e **em que ordem**; o que cada etapa faz mora ao lado,
onde pode ser lido e testado sem subir o Airflow.

## Segurança

- Nenhuma senha no código ou no `docker-compose.yml`. A senha do banco
  chega por variável de ambiente e vira uma credencial do Airflow na
  inicialização; a rotina conhece só o nome dela.
- O arquivo `.env` está no `.gitignore`. O que vai para o GitHub é o
  `.env.example`, com valores de exemplo.
- A tela do Airflow está sem senha. Isso vale **só** para o ambiente local
  deste repositório.

## Problemas comuns

**"ports are not available" ou "bind: An attempt was made to access a socket
in a way forbidden by its access permissions".** O Windows reserva faixas de
porta para uso próprio, e elas mudam a cada reinício. Veja quais estão
reservadas com:

```bash
netsh int ipv4 show excludedportrange protocol=tcp
```

Se a porta do projeto estiver numa das faixas, mude `POSTGRES_PORT` no
arquivo `.env` para qualquer valor livre abaixo de 49152 e suba de novo.

**A rotina não aparece na interface.** Veja o registro do leitor de
rotinas:

```bash
docker compose logs airflow-dag-processor | grep -i error
```

**Erro `cryptography.fernet.InvalidToken` na subida.** A
`AIRFLOW_FERNET_KEY` mudou entre duas subidas, e as senhas guardadas antes
não podem mais ser lidas. Fixe a chave no `.env` e recrie com
`docker compose down -v`.

**A API não responde.** A tarefa `aguardar_api` tenta doze vezes antes de
desistir. Se ainda assim falhar:

```bash
docker compose logs api-clima
curl http://localhost:18000/saude
```

**Uma execução fica presa em "running".** Costuma ser um teste
interrompido no meio. Rodar de novo resolve, já que a rotina é segura para
repetir.

Mais casos em [docs/operacao.md](docs/operacao.md).

## O que precisaria mudar para produção

- **Autenticação de verdade** na interface, no lugar do modo aberto.
- **Outro executor.** O atual roda as tarefas dentro do próprio
  agendador: simples e suficiente aqui, mas não escala nem isola falhas.
- **Imagem própria** com as dependências fixadas, em vez da imagem oficial
  com pastas montadas.
- **Registros de execução em armazenamento remoto**, não numa pasta local.
- **Alerta de verdade.** A rotina falha e fica vermelha na tela, mas
  ninguém é avisado. Faltaria um aviso por e-mail, Slack ou webhook.
- **Menos dados trafegando entre tarefas.** As medições passam de uma
  tarefa para outra pelo mecanismo interno do Airflow, o que é adequado
  para os cerca de 190 registros diários desta origem. Com milhões de
  linhas, o certo seria gravar num local intermediário e passar só o
  endereço.

## Limitações

- A API de origem é simulada. Os dados têm sazonalidade e perfil próprio
  por estação, mas a chuva não tem estação — chove igual em julho e em
  janeiro.
- A rotina não processa o histórico automaticamente na primeira subida.
  Carregar meses antigos exige um comando específico, descrito em
  [docs/operacao.md](docs/operacao.md).
- Há só uma rotina. Dependência entre rotinas, que é onde a orquestração
  fica realmente interessante, ficou de fora.

---

## 👤 Autor

Desenvolvido por **Caio Vinícius Barbosa Barros**.

Se você tiver dúvidas, sugestões ou quiser reportar um problema, sinta-se à vontade para entrar em contato:

*   **✉️ E-mail:** [caio@dynamicmotioncentury.com.br](mailto:caio@dynamicmotioncentury.com.br)
*   **🌐 Site/Portfólio:** [www.dynamicmotioncentury.com.br](https://dynamicmotioncentury.com.br)
*   **💼 LinkedIn:** [linkedin.com/in/caio-vinicius-dmc](https://linkedin.com/in/caio-vinicius-dmc)
*   **🐙 GitHub:** [@caio-vinicius-dmc](https://github.com/caio-vinicius-dmc)

💡 *Se este projeto te ajudou, deixe uma ⭐ no repositório!*
