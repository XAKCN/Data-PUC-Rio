# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
"""
Gold - Star schema + analise final (versao Databricks)
Le workspace.silver.inadimplencia_bcb, grava fato_inadimplencia + dim_tempo +
dim_segmento como tabelas Delta em workspace.gold, e imprime as respostas
das 5 perguntas de negocio (print e suficiente para print-screen).
"""
import pandas as pd

SILVER_TABLE = "workspace.silver.inadimplencia_bcb"
GOLD_SCHEMA = "workspace.gold"

df = spark.table(SILVER_TABLE).toPandas()
df["data"] = pd.to_datetime(df["data"])

# ---------- dim_tempo ----------
dim_tempo = pd.DataFrame({"data": sorted(df["data"].unique())})
dim_tempo["data_id"] = dim_tempo["data"].dt.strftime("%Y%m").astype(int)
dim_tempo["ano"] = dim_tempo["data"].dt.year
dim_tempo["mes"] = dim_tempo["data"].dt.month
dim_tempo["trimestre"] = dim_tempo["data"].dt.quarter
dim_tempo = dim_tempo[["data_id", "data", "ano", "mes", "trimestre"]]

# ---------- dim_segmento ----------
descricoes = {
    21082: "Inadimplencia total do SFN (credito livre + direcionado)",
    21083: "Inadimplencia total - Pessoa Juridica",
    21084: "Inadimplencia total - Pessoa Fisica",
    21129: "Inadimplencia - Pessoa Fisica - Cartao de credito (recursos livres)",
    21104: "Inadimplencia - Pessoa Juridica - Cartao de credito rotativo (recursos livres)",
    21146: "Inadimplencia - Pessoa Fisica - Credito rural com taxas de mercado (recursos direcionados)",
}
dim_segmento = df[["codigo_sgs", "segmento", "modalidade"]].drop_duplicates().reset_index(drop=True)
dim_segmento["segmento_id"] = dim_segmento["codigo_sgs"]
dim_segmento["descricao"] = dim_segmento["codigo_sgs"].map(descricoes)
dim_segmento = dim_segmento[["segmento_id", "codigo_sgs", "segmento", "modalidade", "descricao"]]

# ---------- fato_inadimplencia ----------
fato = df.copy()
fato["data_id"] = fato["data"].dt.strftime("%Y%m").astype(int)
fato["segmento_id"] = fato["codigo_sgs"]
fato_inadimplencia = fato[["data_id", "segmento_id", "valor_pct"]]

# grava as 3 tabelas Delta
spark.createDataFrame(dim_tempo).write.mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.dim_tempo")
spark.createDataFrame(dim_segmento).write.mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.dim_segmento")
spark.createDataFrame(fato_inadimplencia).write.mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.fato_inadimplencia")

print("=== CRITICA GOLD ===")
print("linhas fato:", len(fato_inadimplencia), "| linhas silver:", len(df),
      "-> bate?", len(fato_inadimplencia) == len(df))
print("segmentos distintos:", dim_segmento["segmento_id"].nunique(), "(esperado: 6)")
print("FKs orfas (segmento_id):",
      (~fato_inadimplencia["segmento_id"].isin(dim_segmento["segmento_id"])).sum())
print("FKs orfas (data_id):",
      (~fato_inadimplencia["data_id"].isin(dim_tempo["data_id"])).sum())

# ================= ANALISE - RESPONDENDO AS PERGUNTAS =================
def serie(cod):
    s = fato_inadimplencia[fato_inadimplencia["segmento_id"] == cod].merge(dim_tempo, on="data_id")
    return s.sort_values("data").set_index("data")["valor_pct"]

total, pf, pj = serie(21082), serie(21084), serie(21083)
pf_cartao, pf_rural, pj_rotativo = serie(21129), serie(21146), serie(21104)

print("\n=== P1: evolucao da inadimplencia total ===")
print(f"Inicio: {total.iloc[0]:.2f}% | minimo: {total.min():.2f}% em {total.idxmin():%Y-%m} "
      f"| ultimo valor: {total.iloc[-1]:.2f}% (maximo da serie: {total.max():.2f}%)")

print("\n=== P2: PF vs PJ ===")
print(f"Ultimo mes -> PF: {pf.iloc[-1]:.2f}% | PJ: {pj.iloc[-1]:.2f}% "
      f"| PF acima de PJ em {(pf > pj).mean()*100:.0f}% dos meses")

print("\n=== P3: cartao de credito PF vs credito rural PF ===")
print(f"Cartao (ultimo mes disponivel): {pf_cartao.iloc[-1]:.2f}% | "
      f"Rural: {pf_rural.iloc[-1]:.2f}%")
print(f"Media historica -> cartao: {pf_cartao.mean():.2f}% | rural: {pf_rural.mean():.2f}%")

print("\n=== P4: sazonalidade (media por mes do ano) ===")
print(total.groupby(total.index.month).mean().round(2).to_string())

print("\n=== P5: maior alta/queda mensal ===")
var = total.diff().dropna()
print("Maior alta:", f"{var.idxmax():%Y-%m}", f"(+{var.max():.2f} p.p.)")
print("Maior queda:", f"{var.idxmin():%Y-%m}", f"({var.min():.2f} p.p.)")