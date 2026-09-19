<div align="center">

# Inadimplência de Crédito no Brasil

### MVP — Pipeline de Dados em arquitetura medalhão no Databricks

[![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-tabelas%20gerenciadas-00ADD4)](https://delta.io/)
[![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-cat%C3%A1logo%20documentado-1B3139)](docs/catalogo_dados.md)
[![Python](https://img.shields.io/badge/Python-pandas%20%7C%20PySpark%20%7C%20matplotlib-3776AB?logo=python&logoColor=white)](scripts/)
[![Fonte](https://img.shields.io/badge/Fonte-BCB%20%2F%20SGS-00843D)](https://dadosabertos.bcb.gov.br/)
[![Licença dos dados](https://img.shields.io/badge/Dados-ODbL-6E6E6E)](https://opendatacommons.org/licenses/odbl/)

[Visão geral](#visão-geral) · [Contexto](#contexto-de-negócios-e-perguntas-etapa-2-e-41) · [Carga](#carga-dos-dados-etapa-42) · [Modelagem](#modelagem-e-catálogo-de-dados-etapa-43) · [Pipeline](#pipeline-de-dados-etapa-44) · [Qualidade](#qualidade-de-dados-etapa-45) · [Análise](#análise-de-dados-etapa-45) · [Autoavaliação](#autoavaliação)

</div>

---

## Visão geral

<table>
<tr>
<td align="center"><b>6</b><br/>séries do BCB</td>
<td align="center"><b>1110</b><br/>registros na Silver</td>
<td align="center"><b>185</b><br/>meses (mar/2011 – jul/2026)</td>
<td align="center"><b>4</b><br/>tabelas Delta</td>
<td align="center"><b>0</b><br/>nulos e duplicatas</td>
</tr>
</table>

| Pergunta | Resposta em uma linha |
|---|---|
| **P1** Evolução da inadimplência total | Fechou jul/2026 em **4,88%**, o valor mais alto da série |
| **P2** PF vs. PJ | PF acima de PJ em **100% dos 185 meses** |
| **P3** Cartão vs. crédito rural (PF) | Rural saiu de 1,88% para **15,53%** e ultrapassou o cartão |
| **P4** Sazonalidade | Fraca: amplitude de apenas **0,29 p.p.** |
| **P5** Maior alta / queda mensal | **+0,30 p.p.** em jul/2026 · **-0,36 p.p.** em jun/2020 |

```mermaid
flowchart LR
    API["API pública do BCB<br/>SGS · 6 séries"] -->|01_extract_bronze| B[("Bronze<br/>bronze/*.csv")]
    B -->|02_transform_silver| S[("Silver<br/>inadimplencia_bcb")]
    S -->|03_quality_checks| Q{{"5 testes<br/>de qualidade"}}
    S -->|04_model_gold_e_analise| G[("Gold · esquema estrela<br/>fato + 2 dimensões")]
    G -->|05_charts| C["Gráficos"]
    G -->|07_comentarios_unity_catalog| UC["Unity Catalog<br/>descrições"]
    B -.->|06_verificacao_final| V["Verificação final"]
    S -.-> V
```

<details>
<summary><b>Estrutura do repositório</b></summary>

```text
.
├── bronze/                 # 6 CSVs brutos da API do BCB + _metadata_ingestao.json
├── scripts/                # notebooks 00–07, executados no Databricks
├── docs/
│   ├── catalogo_dados.md   # catálogo completo: tipos, domínios e linhagem
│   └── evidencias/         # prints de execução
├── charts/                 # gráficos gerados pelo 05_charts.py
└── README.md
```

</details>

---

## Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

> **Problema:** entender como a inadimplência de crédito no Brasil evoluiu ao longo do tempo, comparando o comportamento entre pessoas físicas e jurídicas e entre diferentes modalidades de crédito.

**Perguntas de negócio:**

| # | Pergunta |
|---|---|
| P1 | Como a taxa de inadimplência total evoluiu nos últimos 15 anos? |
| P2 | Pessoas físicas têm taxa de inadimplência maior que pessoas jurídicas? |
| P3 | Como se comparam duas modalidades específicas — cartão de crédito (PF) e crédito rural (PF)? |
| P4 | Existe sazonalidade na inadimplência ao longo do ano? |
| P5 | Quais foram os períodos de maior alta/queda mensal registrados? |

**Fonte dos dados:** Banco Central do Brasil — Sistema Gerenciador de Séries Temporais (SGS), portal `dadosabertos.bcb.gov.br`. Seis séries temporais mensais, todas com o mesmo conceito (percentual da carteira de crédito com parcela em atraso superior a 90 dias), recortadas por segmento e modalidade.

Cada série bruta chega da API com apenas duas colunas: `data` (mês de referência, formato `dd/mm/aaaa`) e `valor` (percentual, com vírgula decimal). A estrutura é idêntica nas 6 séries — a diferença entre elas está só no recorte de segmento/modalidade, por isso o significado de cada uma precisou ser documentado à parte (ver [Catálogo de Dados](docs/catalogo_dados.md)):

| Código SGS | Segmento | Modalidade |
|:---:|---|---|
| `21082` | Total (SFN) | Todas |
| `21083` | Pessoa Jurídica | Todas |
| `21084` | Pessoa Física | Todas |
| `21129` | Pessoa Física | Cartão de crédito |
| `21104` | Pessoa Jurídica | Cartão de crédito rotativo |
| `21146` | Pessoa Física | Crédito rural |

> [!NOTE]
> **Licença:** Open Data Commons Open Database License (ODbL) — dados abertos, sem necessidade de autenticação, reutilização livre com atribuição à fonte.

---

## Carga dos Dados (Etapa 4.2)

Coleta feita via **API pública do BCB** (caso avançado — requisição HTTP, não apenas upload de arquivo estático), padrão:

```http
GET https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=csv
```

**Script:** [`scripts/01_extract_bronze.py`](scripts/01_extract_bronze.py) — documenta como os dados foram obtidos originalmente (chamada à API do BCB). Os 6 CSVs resultantes ficam versionados diretamente na pasta `bronze/` deste repositório e são lidos pelos notebooks a partir de um caminho relativo (`../bronze`), sem depender de Volume do Unity Catalog — mais simples para o escopo deste MVP.

O hash (sha256) e a contagem de linhas de cada arquivo Bronze são calculados em `scripts/06_verificacao_final.py`; a validação dos registros está na seção de [Qualidade](#qualidade-de-dados-etapa-45).

---

## Modelagem e Catálogo de Dados (Etapa 4.3)

Arquitetura em camadas (medalhão): **Bronze → Silver → Gold**, com o Gold modelado em **esquema estrela**:

- `fato_inadimplencia` (grão: 1 série × 1 mês)
- `dim_tempo` (data, ano, mês, trimestre)
- `dim_segmento` (segmento, modalidade, descrição de negócio)

```mermaid
erDiagram
    dim_tempo ||--o{ fato_inadimplencia : "data_id"
    dim_segmento ||--o{ fato_inadimplencia : "segmento_id"
    fato_inadimplencia {
        bigint data_id FK
        bigint segmento_id FK
        double valor_pct
    }
    dim_tempo {
        bigint data_id PK
        date data
        int ano
        int mes
        int trimestre
    }
    dim_segmento {
        bigint segmento_id PK
        bigint codigo_sgs
        string segmento
        string modalidade
        string descricao
    }
```

Catálogo de dados completo (todas as tabelas, campos, tipos, domínios e linhagem): [`docs/catalogo_dados.md`](docs/catalogo_dados.md).

<p align="center">
  <img src="docs/evidencias/01_unity_catalog.png" alt="Catálogo de dados no Unity Catalog" width="100%"/>
  <br/>
  <sub>Descrições de tabelas e colunas preenchidas via <a href="scripts/07_comentarios_unity_catalog.sql"><code>scripts/07_comentarios_unity_catalog.sql</code></a>.</sub>
</p>

---

## Pipeline de Dados (Etapa 4.4)

Pipeline dividido em scripts, um por camada, para manter clareza e permitir reexecução independente (rodar como notebooks no Databricks, na ordem abaixo):

| Script | Camada | O que faz |
|---|:---:|---|
| [`00_setup.sql`](scripts/00_setup.sql) | — | Cria os schemas `workspace.silver` e `workspace.gold` (uma vez) |
| [`02_transform_silver.py`](scripts/02_transform_silver.py) | 🥈 Silver | Lê `../bronze`, tipa datas e valores, remove duplicatas, grava `workspace.silver.inadimplencia_bcb` |
| [`03_quality_checks.py`](scripts/03_quality_checks.py) | — | Roda os 5 testes de qualidade sobre a tabela Silver |
| [`04_model_gold_e_analise.py`](scripts/04_model_gold_e_analise.py) | 🥇 Gold | Constrói `fato_inadimplencia`, `dim_tempo`, `dim_segmento` e imprime as respostas às 5 perguntas |
| [`05_charts.py`](scripts/05_charts.py) | — | Gera os 2 gráficos direto na saída da célula |
| [`06_verificacao_final.py`](scripts/06_verificacao_final.py) | — | Calcula hash e contagem de linhas dos CSVs Bronze, confirma que as 4 tabelas existem e compara a contagem de linhas da Silver recalculada com a tabela gravada |

**Log real da transformação Silver** (evidência de execução):

```text
=== LOG DE TRANSFORMACOES (Silver) ===
- serie 21082 (total/todas): 185 linhas brutas -> 185 linhas silver (0 duplicata(s) de data removida(s))
- serie 21083 (pessoa_juridica/todas): 185 linhas brutas -> 185 linhas silver (0 duplicata(s) de data removida(s))
- serie 21084 (pessoa_fisica/todas): 185 linhas brutas -> 185 linhas silver (0 duplicata(s) de data removida(s))
- serie 21129 (pessoa_fisica/cartao_credito): 185 linhas brutas -> 185 linhas silver (0 duplicata(s) de data removida(s))
- serie 21104 (pessoa_juridica/cartao_credito_rotativo): 185 linhas brutas -> 185 linhas silver (0 duplicata(s) de data removida(s))
- serie 21146 (pessoa_fisica/credito_rural): 185 linhas brutas -> 185 linhas silver (0 duplicata(s) de data removida(s))

Tabela gravada: workspace.silver.inadimplencia_bcb
Total de linhas: 1110
Periodo coberto: 2011-03-01 a 2026-07-01
```

**Crítica do Gold** (integridade referencial validada por script, não assumida):

```text
linhas fato: 1110 | linhas silver: 1110 -> bate? True
segmentos distintos: 6 (esperado: 6)
FKs orfas (segmento_id): 0
FKs orfas (data_id): 0
```

<p align="center">
  <img src="docs/evidencias/02_tabelas_persistidas.png" alt="Tabelas persistidas no Databricks" width="60%"/>
</p>

---

## Qualidade de Dados (Etapa 4.5)

**Script:** [`scripts/03_quality_checks.py`](scripts/03_quality_checks.py)

| Dimensão | Resultado |
|---|---|
| ✅ **Completude** | 0 valores nulos em qualquer coluna. Nenhum mês faltando no meio de nenhuma série. |
| ✅ **Consistência** | 0 datas com falha de parsing; 100% das datas caem no dia 1 do mês; periodicidade mensal respeitada em todas as séries. |
| ✅ **Unicidade** | 0 linhas duplicadas (mesma série + mesma data). |
| ✅ **Acurácia** | Todos os valores dentro do domínio esperado [0%, 100%] (mínimo 0,36%, máximo 62,70%). |
| ⚠️ **Outliers** | Detectados via regra IQR por série: 26 no total, **não tratados como erro**: (1) meses de 2026 nas séries Total (5) e PF (4), que refletem uma alta real e recente; (2) a série de Crédito Rural (PF), com 13 outliers entre jul/2025 e jul/2026, que sai de 0,36%–0,61% em 2022 para 15,53% em jul/2026 — um evento econômico real, não um erro de captura; (3) PF em mai–jun/2012 (2) e PJ em abr–mai/2017 (2). Decisão documentada: manter os valores como estão. |

---

## Análise de Dados (Etapa 4.5)

![Total, PF e PJ](charts/01_total_pf_pj.png)

**P1 — Evolução da inadimplência total:** partiu de 3,17% em mar/2011, atingiu a mínima histórica de 2,16% em dez/2020 (efeito das renegociações e liquidez do período de pandemia) e fechou jul/2026 em **4,88% — o valor mais alto de toda a série histórica**, acima do pico de 2017 (4,11%), que já havia sido superado em ago/2025 (4,14%).

**P2 — Pessoa física vs. jurídica:** PF esteve **acima de PJ em 100% dos 185 meses** analisados. Em jul/2026, PF está em 5,81% contra 3,31% de PJ (razão de 1,76x). A média histórica é 4,09% (PF) vs. 2,41% (PJ).

![Cartão de crédito vs Crédito rural](charts/02_cartao_vs_rural.png)

> [!IMPORTANT]
> **P3 — Cartão de crédito vs. crédito rural (PF):** durante quase toda a série (até meados de 2025), o cartão de crédito foi consistentemente a modalidade mais crítica (média histórica de 7,20% vs. 3,09% do rural). Isso **se inverteu recentemente**: o crédito rural saiu de 1,88% em jun/2024 para 15,53% em jul/2026 (+13,65 p.p. em 25 meses) e **ultrapassou o cartão de crédito** (9,51% em jul/2026) — o achado mais forte desta análise, provavelmente ligado a estresse no agronegócio nesse período.

**P4 — Sazonalidade:** fraca. A média mensal da série Total varia pouco ao longo do ano (mínima em dezembro, 3,15%; máxima em maio, 3,44%) — uma amplitude de apenas 0,29 p.p., não caracterizando um padrão sazonal forte.

**P5 — Maior alta/queda mensal:** a maior alta mensal individual foi em jul/2026 (+0,30 p.p., o mês mais recente da série). A maior queda mensal foi em jun/2020 (-0,36 p.p.), coincidindo com os programas de renegociação de crédito durante a pandemia.

<p align="center">
  <img src="docs/evidencias/03_saida_perguntas.png" alt="Respostas às perguntas de negócio" width="70%"/>
</p>

---

## Autoavaliação

Das cinco perguntas formuladas na etapa inicial, todas foram respondidas — inclusive a P4, sobre sazonalidade, que veio praticamente negativa: a amplitude entre os meses foi de apenas 0,29 p.p., o que não caracteriza um padrão sazonal relevante. Mantive a pergunta no documento mesmo assim, porque um resultado negativo continua sendo uma resposta.

A que mais me marcou foi a P3: eu a escrevi partindo do pressuposto de que o cartão de crédito seria a modalidade com maior inadimplência, e os dados mais recentes mostraram o contrário — o crédito rural PF saiu de 1,88% em jun/2024 para 15,53% em jul/2026, ultrapassando o cartão. A primeira impressão foi de que poderia se tratar de alguma anomalia nos dados, mas não era: a alta foi gradual ao longo de 25 meses, os testes de completude, consistência e unicidade não encontraram problemas na série e os valores conferem com a API do BCB. A principal hipótese que explica esse efeito abrupto é que a elevação da inadimplência do crédito rural para pessoas físicas a partir de 2024 decorre da combinação entre a forte alta dos juros, a queda nos preços das commodities, a persistência de custos de produção elevados, os impactos de eventos climáticos adversos e o excessivo endividamento acumulado durante o ciclo de bonança de 2020-2023.
Percebi, depois, que a série 21146 é classificada como "pessoa física", mas o devedor ali é o produtor rural — não uma família consumindo no cartão. O recorte PF do BCB agrega perfis econômicos bem diferentes, e isso só ficou claro para mim depois de ver os dados.

### Dificuldades encontradas

A parte mais desafiadora da criação do pipeline foi descobrir onde o Databricks permite escrita (Volume, workspace file, disco local). Precisei pesquisar como a plataforma se comporta, e cheguei a rodar a extração gravando os arquivos num caminho diferente do que a etapa Silver lia: o pipeline executou sem erro, mas com os dados antigos, e só percebi porque o log da Silver continuava mostrando 184 linhas para a série 21129.

Essa mesma série trouxe outra dificuldade. Na primeira coleta, ela estava um mês defasada em relação às demais, e precisei decidir se era erro de coleta ou comportamento normal da fonte. Ao coletar de novo, o BCB já tinha publicado jul/2026 e revisado meses anteriores (o cartão de crédito PF de jun/2026 passou de 9,59% para 9,09%). Isso me mostrou que dados públicos podem mudar depois de publicados, e que o resultado da análise vale para a data da coleta.

Também precisei decidir não tratar os outliers do crédito rural: eles eram anômalos estatisticamente, mas reais economicamente. Pesquisei o contexto econômico para entender esse aumento repentino, mas a causa não pode ser confirmada com os dados deste MVP. Reforço que essa não era a ideia central — eu pretendia analisar a carteira de cartão de crédito PF, e não a de crédito rural.

### Limitações

As transformações foram feitas em pandas, e o Spark foi usado apenas para ler e gravar as tabelas; com 1110 linhas isso funciona, mas não aproveita o processamento distribuído da plataforma. Na P4, calculei a média por mês do ano sem remover a tendência de longo prazo, o que é uma forma simples de olhar sazonalidade.

### Trabalhos futuros

O pipeline mostra que o crédito rural disparou, mas não por quê. Cruzar essas séries com Selic, preços de commodities ou dados de recuperação judicial permitiria testar as hipóteses que levantei aqui, em vez de apenas sugeri-las.

---

<div align="center">
<sub>MVP da Sprint de Engenharia de Dados · Pós-graduação PUC-Rio · Dados: Banco Central do Brasil (SGS), licença ODbL</sub>
</div>
