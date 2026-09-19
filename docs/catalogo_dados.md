# Catálogo de Dados — Inadimplência de Crédito no Brasil

## Camada Bronze

6 arquivos CSV, um por série temporal do SGS/BCB, dado exatamente como recebido da API (separador `;`, decimal `,`).

**Arquivos:** `bronze/<codigo_sgs>_inadimplencia_<recorte>.csv` (um arquivo por série)

| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| `data` | string (`dd/mm/aaaa`) | Data de referência do mês, sempre dia 1 | `01/03/2011` a `01/07/2026` |
| `valor` | string (decimal com vírgula) | Percentual de inadimplência bruto, como veio da API | `"0,36"` a `"62,70"` |

**Linhagem:** extraído via API pública do BCB (`GET https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=csv`), sem nenhuma transformação. Licença: Open Data Commons Open Database License (ODbL).

---

## Camada Silver

**Tabela:** `silver.inadimplencia_bcb`

| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| `data` | date | Data de referência (mês), tipada a partir do bronze | 2011-03-01 a 2026-07-01 |
| `codigo_sgs` | bigint | Código da série no SGS/BCB, chave de origem | {21082, 21083, 21084, 21104, 21129, 21146} |
| `segmento` | string | Segmento do tomador de crédito | {total, pessoa_fisica, pessoa_juridica} |
| `modalidade` | string | Modalidade de crédito específica | {todas, cartao_credito, cartao_credito_rotativo, credito_rural} |
| `valor_pct` | double | Percentual de inadimplência, tipado (vírgula → ponto) | 0,36 a 62,70 |

**Linhagem:** concatenação dos 6 CSVs Bronze após parse de data, conversão de decimal e enriquecimento com `segmento`/`modalidade` a partir da lista de séries definida em `scripts/02_transform_silver.py`. Deduplicação por (`codigo_sgs`, `data`) aplicada (0 duplicatas encontradas).

---

## Camada Gold — Esquema Estrela

**`fato_inadimplencia`** (grão: 1 linha = 1 série × 1 mês)

| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| `data_id` | bigint | FK para `dim_tempo` (formato AAAAMM) | 201103 a 202607 |
| `segmento_id` | bigint | FK para `dim_segmento` (= `codigo_sgs`) | {21082, 21083, 21084, 21104, 21129, 21146} |
| `valor_pct` | double | Percentual de inadimplência do mês | 0,36 a 62,70 |

**`dim_tempo`**

| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| `data_id` | bigint | Chave primária (AAAAMM) | 201103 a 202607 |
| `data` | date | Primeiro dia do mês | 2011-03-01 a 2026-07-01 |
| `ano` | int | Ano de referência | 2011 a 2026 |
| `mes` | int | Mês de referência | 1 a 12 |
| `trimestre` | int | Trimestre do ano | 1 a 4 |

**`dim_segmento`**

| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| `segmento_id` | bigint | Chave primária (= código SGS) | {21082, 21083, 21084, 21104, 21129, 21146} |
| `codigo_sgs` | bigint | Código oficial da série no BCB | idem |
| `segmento` | string | Tipo de tomador | {total, pessoa_fisica, pessoa_juridica} |
| `modalidade` | string | Modalidade de crédito | {todas, cartao_credito, cartao_credito_rotativo, credito_rural} |
| `descricao` | string | Descrição de negócio completa da série | texto livre |

**Linhagem:** `fato_inadimplencia` é uma projeção direta da tabela Silver; `dim_tempo` e `dim_segmento` são derivadas dos valores distintos da própria Silver, mantendo integridade referencial (validado: 0 chaves órfãs em ambas as dimensões).
