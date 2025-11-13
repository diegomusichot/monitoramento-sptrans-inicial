#!/usr/bin/env python
"""
Script: carregar_gtfs_paradas.py
Objetivo:
  - Ler o arquivo stops.txt do GTFS da SPTrans
  - Selecionar as colunas principais das paradas
  - Salvar em formato Parquet na camada Silver (paradas)
"""

from pathlib import Path
import pandas as pd

# Detecta se está rodando dentro do container do Airflow
INSIDE_DOCKER = Path("/opt/airflow").exists()

BASE = Path("/opt/airflow/data_lake") if INSIDE_DOCKER else Path("data_lake")

# Pasta onde o GTFS foi extraído
GTFS_DIR = BASE / "reference" / "gtfs"

# Pasta de saída para as paradas (Silver)
PARADAS_SILVER = BASE / "silver" / "sptrans_paradas"


def main():
    print(f"[info] GTFS_DIR: {GTFS_DIR.resolve()}")
    print(f"[info] PARADAS_SILVER: {PARADAS_SILVER.resolve()}")

    stops_path = GTFS_DIR / "stops.txt"
    if not stops_path.exists():
        print("[falha] Arquivo stops.txt não encontrado. Verifique se o GTFS foi extraído em data_lake/reference/gtfs.")
        return

    # ----------------------------------------------------------------
    # 1) Ler o stops.txt com Pandas
    # ----------------------------------------------------------------
    # GTFS usa separador vírgula e geralmente codificação UTF-8
    df = pd.read_csv(stops_path)

    print(f"[info] Colunas encontradas em stops.txt: {list(df.columns)}")
    print(f"[info] Total de paradas no GTFS: {len(df)}")

    # ----------------------------------------------------------------
    # 2) Selecionar colunas principais (ajuste conforme as colunas existentes)
    # ----------------------------------------------------------------
    colunas_basicas = []

    # Essas colunas são padrão GTFS; só adiciona se existirem de fato
    for c in ["stop_id", "stop_code", "stop_name", "stop_desc",
              "stop_lat", "stop_lon", "zone_id", "stop_url",
              "location_type", "parent_station"]:
        if c in df.columns:
            colunas_basicas.append(c)

    if not colunas_basicas:
        print("[falha] Nenhuma coluna padrão de paradas encontrada. Verifique o formato do stops.txt.")
        return

    df_paradas = df[colunas_basicas].copy()

    # ----------------------------------------------------------------
    # 3) Limpeza simples / tipos
    # ----------------------------------------------------------------
    # Garantir que latitude e longitude sejam numéricos
    if "stop_lat" in df_paradas.columns:
        df_paradas["stop_lat"] = pd.to_numeric(df_paradas["stop_lat"], errors="coerce")
    if "stop_lon" in df_paradas.columns:
        df_paradas["stop_lon"] = pd.to_numeric(df_paradas["stop_lon"], errors="coerce")

    # Remove paradas sem coordenada
    if "stop_lat" in df_paradas.columns and "stop_lon" in df_paradas.columns:
        antes = len(df_paradas)
        df_paradas = df_paradas.dropna(subset=["stop_lat", "stop_lon"])
        depois = len(df_paradas)
        print(f"[info] Paradas com coordenadas válidas: {depois} (removidas {antes - depois} sem lat/lon)")

    # ----------------------------------------------------------------
    # 4) Salvar em Parquet na camada Silver de paradas
    # ----------------------------------------------------------------
    PARADAS_SILVER.mkdir(parents=True, exist_ok=True)
    out_path = PARADAS_SILVER / "paradas_gtfs.parquet"

    df_paradas.to_parquet(out_path, index=False)
    print(f"[ok] Arquivo de paradas GTFS salvo em: {out_path}")


if __name__ == "__main__":
    main()
 
