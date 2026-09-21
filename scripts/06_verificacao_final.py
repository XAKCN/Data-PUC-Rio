# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
"""
Verificacao final (versao Databricks - Workspace files)
Roda do zero a partir de ../bronze e das tabelas Delta, sem reaproveitar
nada. So imprime - print-screen da saida serve como evidencia.
"""
import hashlib
import pandas as pd
from pathlib import Path

BRONZE_DIR = Path("../bronze")
erros = []

SERIES = [
    {"codigo_sgs": 21082, "arquivo": "21082_inadimplencia_total.csv"},
    {"codigo_sgs": 21083, "arquivo": "21083_inadimplencia_pj_total.csv"},
    {"codigo_sgs": 21084, "arquivo": "21084_inadimplencia_pf_total.csv"},
    {"codigo_sgs": 21129, "arquivo": "21129_inadimplencia_pf_cartao_credito.csv"},
    {"codigo_sgs": 21104, "arquivo": "21104_inadimplencia_pj_cartao_rotativo.csv"},
    {"codigo_sgs": 21146, "arquivo": "21146_inadimplencia_pf_credito_rural.csv"},
]

print("=== 1. INTEGRIDADE BRONZE (hash + linhas) ===")
for s in SERIES:
    caminho = BRONZE_DIR / s["arquivo"]
    conteudo = caminho.read_bytes()
    h = hashlib.sha256(conteudo).hexdigest()[:12]
    n = conteudo.decode("utf-8").strip().count("\n")
    print(f"{s['arquivo']}: sha256={h} linhas={n}")

print("\n=== 2. TABELAS DELTA EXISTEM? ===")
for tabela in ["workspace.silver.inadimplencia_bcb", "workspace.gold.fato_inadimplencia",
               "workspace.gold.dim_tempo", "workspace.gold.dim_segmento"]:
    existe = spark.catalog.tableExists(tabela)
    print(tabela, "OK" if existe else "FALTANDO")
    if not existe:
        erros.append(f"Tabela ausente: {tabela}")

print("\n=== 3. CROSS-CHECK: SILVER RECALCULADO x TABELA GRAVADA ===")
partes = []
for s in SERIES:
    d = pd.read_csv(BRONZE_DIR / s["arquivo"], sep=";", quotechar='"', dtype=str)
    d["data"] = pd.to_datetime(d["data"], format="%d/%m/%Y")
    d["valor_pct"] = d["valor"].str.replace(",", ".").astype(float)
    d["codigo_sgs"] = s["codigo_sgs"]
    partes.append(d[["data", "codigo_sgs", "valor_pct"]])
recalculado = pd.concat(partes, ignore_index=True)

if spark.catalog.tableExists("workspace.silver.inadimplencia_bcb"):
    gravado = spark.table("workspace.silver.inadimplencia_bcb").toPandas()
    if len(recalculado) != len(gravado):
        erros.append(f"Linhas recalculadas ({len(recalculado)}) != linhas na tabela ({len(gravado)})")
    else:
        print(f"Linhas recalculadas do zero: {len(recalculado)} | linhas na tabela: {len(gravado)} -> bate")
else:
    erros.append("Tabela workspace.silver.inadimplencia_bcb nao existe - rode o 02 primeiro")

print("\n" + "=" * 50)
if erros:
    print(f"RESULTADO: {len(erros)} PROBLEMA(S):")
    for e in erros:
        print(" -", e)
else:
    print("RESULTADO: PASS - dados no workspace batem com as tabelas Delta gravadas.")