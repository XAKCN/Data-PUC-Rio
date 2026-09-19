-- ============================================================
-- Catalogo de Dados no Unity Catalog
-- Preenche descricoes de tabelas e colunas, para que o Catalog
-- Explorer reflita o catalogo documentado em docs/catalogo_dados.md
-- ============================================================

-- ---------- SILVER ----------
COMMENT ON TABLE workspace.silver.inadimplencia_bcb IS
'Camada Silver: uniao das 6 series de inadimplencia do BCB/SGS, ja tipadas e padronizadas. Formato long, um registro por serie por mes. Fonte: Banco Central do Brasil (SGS), licenca ODbL.';

ALTER TABLE workspace.silver.inadimplencia_bcb ALTER COLUMN data
  COMMENT 'Data de referencia do mes (sempre dia 1). Dominio: 2011-03-01 a 2026-07-01. Derivado do campo data do Bronze (dd/mm/aaaa).';
ALTER TABLE workspace.silver.inadimplencia_bcb ALTER COLUMN codigo_sgs
  COMMENT 'Codigo da serie no Sistema Gerenciador de Series Temporais do BCB. Dominio: 21082, 21083, 21084, 21104, 21129, 21146.';
ALTER TABLE workspace.silver.inadimplencia_bcb ALTER COLUMN segmento
  COMMENT 'Segmento do tomador de credito. Dominio: total, pessoa_fisica, pessoa_juridica.';
ALTER TABLE workspace.silver.inadimplencia_bcb ALTER COLUMN modalidade
  COMMENT 'Modalidade de credito. Dominio: todas, cartao_credito, cartao_credito_rotativo, credito_rural.';
ALTER TABLE workspace.silver.inadimplencia_bcb ALTER COLUMN valor_pct
  COMMENT 'Percentual da carteira de credito com pelo menos uma parcela em atraso superior a 90 dias. Dominio observado: 0,36 a 62,70. Derivado do campo valor do Bronze (virgula decimal convertida para ponto).';

-- ---------- GOLD: fato ----------
COMMENT ON TABLE workspace.gold.fato_inadimplencia IS
'Tabela fato do esquema estrela. Grao: 1 serie x 1 mes. Metrica: percentual de inadimplencia. Derivada diretamente da camada Silver.';

ALTER TABLE workspace.gold.fato_inadimplencia ALTER COLUMN data_id
  COMMENT 'Chave estrangeira para dim_tempo, no formato AAAAMM. Dominio: 201103 a 202607.';
ALTER TABLE workspace.gold.fato_inadimplencia ALTER COLUMN segmento_id
  COMMENT 'Chave estrangeira para dim_segmento (equivale ao codigo da serie no SGS). Dominio: 21082, 21083, 21084, 21104, 21129, 21146.';
ALTER TABLE workspace.gold.fato_inadimplencia ALTER COLUMN valor_pct
  COMMENT 'Percentual da carteira de credito com atraso superior a 90 dias no mes. Dominio observado: 0,36 a 62,70.';

-- ---------- GOLD: dim_tempo ----------
COMMENT ON TABLE workspace.gold.dim_tempo IS
'Dimensao de tempo, granularidade mensal. Derivada dos valores distintos de data presentes na camada Silver.';

ALTER TABLE workspace.gold.dim_tempo ALTER COLUMN data_id
  COMMENT 'Chave primaria da dimensao, no formato AAAAMM. Dominio: 201103 a 202607.';
ALTER TABLE workspace.gold.dim_tempo ALTER COLUMN data
  COMMENT 'Primeiro dia do mes de referencia. Dominio: 2011-03-01 a 2026-07-01.';
ALTER TABLE workspace.gold.dim_tempo ALTER COLUMN ano
  COMMENT 'Ano de referencia. Dominio: 2011 a 2026.';
ALTER TABLE workspace.gold.dim_tempo ALTER COLUMN mes
  COMMENT 'Mes de referencia. Dominio: 1 a 12.';
ALTER TABLE workspace.gold.dim_tempo ALTER COLUMN trimestre
  COMMENT 'Trimestre do ano. Dominio: 1 a 4.';

-- ---------- GOLD: dim_segmento ----------
COMMENT ON TABLE workspace.gold.dim_segmento IS
'Dimensao de segmento/modalidade de credito. Cada linha descreve uma das 6 series do BCB utilizadas no MVP.';

ALTER TABLE workspace.gold.dim_segmento ALTER COLUMN segmento_id
  COMMENT 'Chave primaria da dimensao (equivale ao codigo da serie no SGS). Dominio: 21082, 21083, 21084, 21104, 21129, 21146.';
ALTER TABLE workspace.gold.dim_segmento ALTER COLUMN codigo_sgs
  COMMENT 'Codigo oficial da serie no Sistema Gerenciador de Series Temporais do Banco Central.';
ALTER TABLE workspace.gold.dim_segmento ALTER COLUMN segmento
  COMMENT 'Tipo de tomador de credito. Dominio: total, pessoa_fisica, pessoa_juridica.';
ALTER TABLE workspace.gold.dim_segmento ALTER COLUMN modalidade
  COMMENT 'Modalidade de credito. Dominio: todas, cartao_credito, cartao_credito_rotativo, credito_rural.';
ALTER TABLE workspace.gold.dim_segmento ALTER COLUMN descricao
  COMMENT 'Descricao de negocio completa da serie, conforme documentacao oficial do BCB.';
