# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
"""
Graficos (versao Databricks) - le as tabelas Gold e desenha os graficos
direto na saida da celula (plt.show()). Nao grava nada em disco: para a
evidencia exigida no trabalho, e so tirar print-screen da celula depois
de rodar.
"""
import pandas as pd
import matplotlib.pyplot as plt

fato = spark.table("workspace.gold.fato_inadimplencia").toPandas()
tempo = spark.table("workspace.gold.dim_tempo").toPandas()
tempo["data"] = pd.to_datetime(tempo["data"])
seg = spark.table("workspace.gold.dim_segmento").toPandas()

df = fato.merge(tempo, on="data_id").merge(seg, on="segmento_id")

def s(cod):
    d = df[df["codigo_sgs"] == cod].sort_values("data")
    return d.set_index("data")["valor_pct"]

plt.style.use("seaborn-v0_8-whitegrid")

# Grafico 1: Total, PF, PJ
fig, ax = plt.subplots(figsize=(11, 5.5))
ax.plot(s(21082).index, s(21082).values, label="Total (SFN)", linewidth=2, color="#333333")
ax.plot(s(21084).index, s(21084).values, label="Pessoa Física", linewidth=2, color="#c0392b")
ax.plot(s(21083).index, s(21083).values, label="Pessoa Jurídica", linewidth=2, color="#2980b9")
ax.set_title("Inadimplência da carteira de crédito - Brasil", fontsize=13, fontweight="bold")
ax.set_ylabel("% da carteira em atraso > 90 dias")
ax.legend(frameon=False)
ax.set_ylim(0, None)
fig.tight_layout()
plt.show()

# Grafico 2: cartao PF vs rural PF
fig, ax = plt.subplots(figsize=(11, 5.5))
ax.plot(s(21129).index, s(21129).values, label="Cartão de crédito (PF)", linewidth=2, color="#8e44ad")
ax.plot(s(21146).index, s(21146).values, label="Crédito rural (PF)", linewidth=2, color="#27ae60")
ax.set_ylim(0, 19)
ax.set_title("Cartão de crédito vs Crédito rural - Inadimplência PF", fontsize=13, fontweight="bold")
ax.set_ylabel("% da carteira em atraso > 90 dias")
ax.legend(frameon=False)
fig.tight_layout()
plt.show()