# Architecture Decisions

Este documento registra as principais decisões de arquitetura e design do projeto **Financial Market Monitor**.

---

## AD-001 — Processamento batch com execução horária

**Decisão**  
Adotar processamento batch com agendamento horário.

**Motivação**  
O objetivo do projeto é consolidar fundamentos de Engenharia de Dados com uma arquitetura simples, previsível e suficiente para monitoramento financeiro recorrente.

**Trade-off**  
- reduz complexidade operacional
- facilita observabilidade e reprocessamento
- não atende cenários que exigem latência em tempo real

---

## AD-002 — Apache Airflow como orquestrador

**Decisão**  
Utilizar Apache Airflow para agendar e orquestrar a ingestão e a transformação dos dados.

**Motivação**  
Era necessário controlar execução recorrente, dependências entre etapas, retries e visibilidade operacional do pipeline.

**Trade-off**  
- melhora controle e rastreabilidade
- facilita evolução futura do pipeline
- adiciona complexidade de infraestrutura em relação a scripts agendados simples

---

## AD-003 — PythonOperator em vez de custom operator

**Decisão**  
Executar os pipelines Python com `PythonOperator`.

**Motivação**  
Para o escopo do projeto, `PythonOperator` entrega simplicidade, clareza e velocidade de implementação sem necessidade de abstrações extras.

**Trade-off**  
- implementação mais rápida
- DAG mais fácil de manter
- menor reaproveitamento caso a solução cresça muito

---

## AD-004 — Upsert com `ON CONFLICT` para garantir idempotência

**Decisão**  
Persistir os dados com `INSERT ... ON CONFLICT DO UPDATE`.

**Motivação**  
O pipeline pode ser reexecutado na mesma janela temporal. Sem upsert, a carga poderia gerar registros duplicados ou desatualizados.

**Trade-off**  
- permite reprocessamento seguro
- evita duplicidade
- tem custo maior do que insert puro em cenários append-only

---

## AD-005 — Chave primária composta por ativo + tempo

**Decisão**  
Modelar as tabelas com chave composta:
- `ticker + datetime` em `stock_quotes`
- `coin_id + datetime` em `crypto_quotes`

**Motivação**  
A cotação é naturalmente identificada pelo ativo e pelo instante de referência.

**Trade-off**  
- reforça unicidade lógica dos dados
- sustenta o upsert de forma consistente
- exige cuidado com a granularidade temporal usada na chave

---

## AD-006 — Snapshot horário para a ingestão de cripto

**Decisão**  
Truncar o timestamp da coleta de cripto para o início da hora.

**Motivação**  
Usar o timestamp completo com minutos, segundos e microssegundos impediria o reaproveitamento da chave e reduziria a efetividade do upsert.

**Trade-off**  
- mesma moeda + mesma hora = atualização
- nova hora = novo registro
- perde granularidade intrahora, mas ganha previsibilidade e idempotência

---

## AD-007 — Uso de duas fontes complementares: yfinance + CoinGecko

**Decisão**  
Usar `yfinance` e `CoinGecko` como fontes complementares de dados.

**Motivação**  
`yfinance` cobre bem ações e parte dos criptoativos no mesmo fluxo. `CoinGecko` complementa a camada cripto com métricas específicas como preço em USD, market cap e volume de 24h.

**Trade-off**  
- amplia cobertura analítica
- reduz dependência de uma única API
- exige padronização e documentação clara das duas fontes

---

## AD-008 — dbt apenas na camada de transformação

**Decisão**  
Usar dbt para staging, integração e marts, mantendo a ingestão fora dele.

**Motivação**  
A ingestão pertence à camada operacional em Python/Airflow. O dbt entra depois, na padronização, modelagem analítica e testes de dados.

**Trade-off**  
- separa claramente ingestão e transformação
- melhora organização e rastreabilidade da camada analítica
- exige disciplina na divisão de responsabilidades do projeto