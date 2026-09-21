# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
"""
Silver - Inadimplencia BCB (versao Databricks - Workspace files)

Le os 6 CSVs de ../bronze (pasta irma de onde este notebook esta salvo,
dentro da mesma pasta do projeto no Workspace) e grava como tabela Delta
gerenciada workspace.silver.inadimplencia_bcb.

Pre-requisito: ja ter rodado 00_setup.sql. Nao precisa de Volume nem de
upload manual - os CSVs ja estao no workspace (pasta bronze/, ao lado de
scripts/, docs/ e charts/).
"""
import pandas as pd
from pathlib import Path

BRONZE_DIR = Path("../bronze")  # relativo a esta notebook (.../scripts/)
SILVER_TABLE = "workspace.silver.inadimplencia_bcb"

SERIES = [
    {"codigo_sgs": 21082, "arquivo": "21082_inadimplencia_total.csv",
     "segmento": "total", "modalidade": "todas"},
    {"codigo_sgs": 21083, "arquivo": "21083_inadimplencia_pj_total.csv",
     "segmento": "pessoa_juridica", "modalidade": "todas"},
    {"codigo_sgs": 21084, "arquivo": "21084_inadimplencia_pf_total.csv",
     "segmento": "pessoa_fisica", "modalidade": "todas"},
    {"codigo_sgs": 21129, "arquivo": "21129_inadimplencia_pf_cartao_credito.csv",
     "segmento": "pessoa_fisica", "modalidade": "cartao_credito"},
    {"codigo_sgs": 21104, "arquivo": "21104_inadimplencia_pj_cartao_rotativo.csv",
     "segmento": "pessoa_juridica", "modalidade": "cartao_credito_rotativo"},
    {"codigo_sgs": 21146, "arquivo": "21146_inadimplencia_pf_credito_rural.csv",
     "segmento": "pessoa_fisica", "modalidade": "credito_rural"},
]

registros = []
log_transformacoes = []

for serie in SERIES:
    caminho = BRONZE_DIR / serie["arquivo"]
    df = pd.read_csv(caminho, sep=";", quotechar='"', dtype=str)

    linhas_brutas = len(df)
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    df["valor_pct"] = df["valor"].str.replace(",", ".", regex=False).astype(float)

    dup_antes = df["data"].duplicated().sum()
    df = df.drop_duplicates(subset="data", keep="first")

    df["codigo_sgs"] = serie["codigo_sgs"]
    df["segmento"] = serie["segmento"]
    df["modalidade"] = serie["modalidade"]

    df = df[["data", "codigo_sgs", "segmento", "modalidade", "valor_pct"]]
    registros.append(df)

    log_transformacoes.append(
        f"serie {serie['codigo_sgs']} ({serie['segmento']}/{serie['modalidade']}): "
        f"{linhas_brutas} linhas brutas -> {len(df)} linhas silver "
        f"({dup_antes} duplicata(s) de data removida(s))"
    )

silver = pd.concat(registros, ignore_index=True).sort_values(["codigo_sgs", "data"])

spark_df = spark.createDataFrame(silver)
spark_df.write.mode("overwrite").saveAsTable(SILVER_TABLE)

print("=== LOG DE TRANSFORMACOES (Silver) ===")
for linha in log_transformacoes:
    print("-", linha)
print(f"\nTabela gravada: {SILVER_TABLE}")
print(f"Total de linhas: {len(silver)}")
print(f"Periodo coberto: {silver['data'].min().date()} a {silver['data'].max().date()}")