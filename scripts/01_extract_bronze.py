"""
Bronze - Extracao via API do Banco Central (SGS)

Este script foi validado ponta a ponta contra a API real do BCB (todas as 6
series retornaram dados corretamente antes deste script ser escrito).

Grava os CSVs em ../bronze (pasta irma de scripts/), o mesmo caminho lido
pelos notebooks 02 e 06.
"""
import json
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

BRONZE_DIR = Path("../bronze")  # relativo a esta notebook (.../scripts/)
BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=csv"

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


def extrair_serie(codigo_sgs: int) -> str:
    """Chama a API publica do BCB (SGS) e retorna o CSV bruto como texto."""
    url = BASE_URL.format(codigo=codigo_sgs)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def main():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    log = []
    for s in SERIES:
        conteudo = extrair_serie(s["codigo_sgs"])
        caminho = BRONZE_DIR / s["arquivo"]
        caminho.write_text(conteudo, encoding="utf-8")
        n_linhas = conteudo.strip().count("\n")  # exclui cabecalho
        log.append(f"serie {s['codigo_sgs']}: {n_linhas} linhas gravadas em {caminho.name}")
        print(log[-1])

    metadata = {
        "fonte": "Banco Central do Brasil - SGS",
        "licenca": "Open Data Commons Open Database License (ODbL)",
        "data_ingestao": datetime.now().strftime("%Y-%m-%d"),
        "endpoint_padrao": BASE_URL,
        "series": SERIES,
    }
    (BRONZE_DIR / "_metadata_ingestao.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
