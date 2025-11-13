#!/usr/bin/env python
"""
Script: carregar_gtfs_linhas.py
Objetivo:
  - Ler o arquivo routes.txt do GTFS da SPTrans
  - Selecionar as colunas principais das linhas (rotas)
  - Salvar em formato Parquet na camada Silver (linhas GTFS)
"""

from pathlib import Path
import pandas as pd

# Detecta se está rodando dentro do container do Airflow ou local
INSIDE_DOCKER = Path("/opt/airflow").exists()
BASE = Path("/opt/airflow/data_lake") if INSIDE_DOCKER else Path("data_lake")

# Pasta onde o GTFS foi extraído
GTFS_DIR = BASE / "reference" / "gtfs"

# Pasta de saída para as linhas (Silver)
LINHAS_SILVER = BASE / "silver" / "sptrans_gtfs"
LINHAS_SILVER.mkdir(parents=True, exist_ok=True)

ARQ_ROTAS = LINHAS_SILVER / "rotas_gtfs.parquet"


def main():
    print(f"[info] GTFS_DIR: {GTFS_DIR.resolve()}")
    print(f"[info] LINHAS_SILVER: {LINHAS_SILVER.resolve()}")

    routes_path = GTFS_DIR / "routes.txt"
    if not routes_path.exists():
        print("[falha] Arquivo routes.txt não encontrado. Verifique se o GTFS foi extraído em data_lake/reference/gtfs.")
        return

    # 1) Ler routes.txt
    df = pd.read_csv(routes_path)
    print(f"[info] Colunas encontradas em routes.txt: {list(df.columns)}")
    print(f"[info] Total de linhas (rotas) no GTFS: {len(df)}")

    # 2) Selecionar colunas mais relevantes
    colunas_basicas = []
    for c in [
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_desc",
        "route_type",
        "route_url",
    ]:
        if c in df.columns:
            colunas_basicas.append(c)

    if not colunas_basicas:
        print("[falha] Nenhuma coluna padrão de rotas encontrada. Verifique o formato de routes.txt.")
        return

    df_rotas = df[colunas_basicas].copy()

    # 3) Salvar em Parquet
    df_rotas.to_parquet(ARQ_ROTAS, index=False)
    print(f"[ok] Arquivo de linhas GTFS salvo em: {ARQ_ROTAS}")


if __name__ == "__main__":
    main()
 
