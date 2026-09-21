# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
"""
Verificacao de qualidade (versao Databricks) - le a tabela Silver e so imprime
resultados na saida da celula (print e o suficiente para print-screen).
"""
import pandas as pd

SILVER_TABLE = "workspace.silver.inadimplencia_bcb"

df = spark.table(SILVER_TABLE).toPandas()
df["data"] = pd.to_datetime(df["data"])

print("=== 1. COMPLETUDE ===")
print(df.isnull().sum().to_string())
buracos = []
for cod, grp in df.groupby("codigo_sgs"):
    meses_esperados = pd.date_range(grp["data"].min(), grp["data"].max(), freq="MS")
    faltando = set(meses_esperados) - set(grp["data"])
    if faltando:
        buracos.append((cod, sorted(faltando)))
print("Meses faltando no MEIO de alguma serie:", buracos if buracos else "nenhum")

print("\n=== 2. CONSISTENCIA ===")
print("Datas com parsing invalido (NaT):", df["data"].isna().sum())
print("Todas as datas caem no dia 1 do mes?", (df["data"].dt.day == 1).all())
freq_ok = df.groupby("codigo_sgs")["data"].apply(
    lambda s: (s.sort_values().diff().dropna().dt.days.between(27, 32)).all()
)
print("Periodicidade mensal respeitada em todas as series?", freq_ok.all())

print("\n=== 3. UNICIDADE ===")
print("Linhas duplicadas (mesma serie + mesma data):",
      df.duplicated(subset=["codigo_sgs", "data"]).sum())

print("\n=== 4. ACURACIA (dominio de valores) ===")
print("valor_pct minimo:", df["valor_pct"].min(), "| maximo:", df["valor_pct"].max())
print("Linhas fora do intervalo [0,100]%:",
      len(df[(df["valor_pct"] < 0) | (df["valor_pct"] > 100)]))

print("\n=== 5. OUTLIERS (por serie, regra IQR) ===")
for cod, grp in df.groupby("codigo_sgs"):
    q1, q3 = grp["valor_pct"].quantile([0.25, 0.75])
    iqr = q3 - q1
    lim_inf, lim_sup = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    out = grp[(grp["valor_pct"] < lim_inf) | (grp["valor_pct"] > lim_sup)]
    print(f"serie {cod}: {len(out)} outlier(s) fora de [{lim_inf:.2f}, {lim_sup:.2f}]%")