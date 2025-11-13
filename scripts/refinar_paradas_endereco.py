#!/usr/bin/env python
"""
Script: refinar_paradas_endereco.py
Objetivo:
  - Ler as paradas do GTFS (paradas_gtfs.parquet)
  - Usar geocodificação reversa (lat/lon -> endereço)
  - Gerar uma tabela de paradas refinadas com endereço
Camadas:
  - Entrada:  data_lake/silver/sptrans_paradas/paradas_gtfs.parquet
  - Saída:    data_lake/silver/sptrans_paradas/paradas_com_endereco.parquet
"""

from pathlib import Path
import time
import requests
import pandas as pd

# Detecta se está rodando dentro do container
INSIDE_DOCKER = Path("/opt/airflow").exists()
BASE = Path("/opt/airflow/data_lake") if INSIDE_DOCKER else Path("data_lake")

PARADAS_SILVER = BASE / "silver" / "sptrans_paradas"
ARQ_PARADAS = PARADAS_SILVER / "paradas_gtfs.parquet"

# Arquivo de saída (paradas refinadas com endereço)
ARQ_PARADAS_ENDERECO = PARADAS_SILVER / "paradas_com_endereco.parquet"

# URL da API de geocodificação reversa (Nominatim / OpenStreetMap)
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"

# IMPORTANTE: coloque um e-mail seu aqui para identificar o uso na API
USER_AGENT = "diegomusichot@gmail.com"


def reverse_geocode(lat: float, lon: float, session: requests.Session):
    """
    Chama a API de geocodificação reversa para uma coordenada (lat, lon)
    Retorna um dicionário com campos de endereço (logradouro, bairro, cidade, uf, cep).
    Em caso de erro, retorna valores None.
    """
    try:
        params = {
            "format": "jsonv2",
            "lat": lat,
            "lon": lon,
            "addressdetails": 1,
        }
        headers = {
            "User-Agent": USER_AGENT
        }

        resp = session.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        addr = data.get("address", {})

        return {
            "endereco_logradouro": addr.get("road") or addr.get("pedestrian") or addr.get("footway"),
            "endereco_bairro": addr.get("suburb") or addr.get("neighbourhood"),
            "endereco_cidade": addr.get("city") or addr.get("town") or addr.get("village"),
            "endereco_uf": addr.get("state"),
            "endereco_cep": addr.get("postcode"),
        }
    except Exception as e:
        print(f"[erro] geocodificando ({lat}, {lon}): {e}")
        return {
            "endereco_logradouro": None,
            "endereco_bairro": None,
            "endereco_cidade": None,
            "endereco_uf": None,
            "endereco_cep": None,
        }


def main():
    print(f"[info] ARQ_PARADAS: {ARQ_PARADAS.resolve()}")
    print(f"[info] ARQ_PARADAS_ENDERECO: {ARQ_PARADAS_ENDERECO.resolve()}")

    if not ARQ_PARADAS.exists():
        print("[falha] Arquivo paradas_gtfs.parquet não encontrado. Rode primeiro o carregar_gtfs_paradas.py.")
        return

    # ------------------------------------------------------------
    # 1) Ler as paradas do GTFS
    # ------------------------------------------------------------
    df = pd.read_parquet(ARQ_PARADAS)
    print(f"[info] Paradas totais lidas: {len(df)}")

    if "stop_lat" not in df.columns or "stop_lon" not in df.columns:
        print("[falha] Colunas stop_lat/stop_lon não encontradas no Parquet de paradas.")
        return

    # ------------------------------------------------------------
    # 2) Identificar coordenadas únicas para reduzir chamadas à API
    # ------------------------------------------------------------
    coords = df[["stop_lat", "stop_lon"]].drop_duplicates().reset_index(drop=True)
    print(f"[info] Coordenadas únicas encontradas: {len(coords)}")

    # OPCIONAL: para testes, limitar o número de coordenadas (ex.: 100 primeiras)
    # Tire esse corte quando estiver confiante e quiser rodar em todas
    # coords = coords.head(100)

    # ------------------------------------------------------------
    # 3) Geocodificar cada coordenada única
    # ------------------------------------------------------------
    resultados = []
    sess = requests.Session()

    for idx, row in coords.iterrows():
        lat = row["stop_lat"]
        lon = row["stop_lon"]

        print(f"[geo] ({idx+1}/{len(coords)}) lat={lat}, lon={lon}")
        info_endereco = reverse_geocode(lat, lon, sess)
        resultados.append({
            "stop_lat": lat,
            "stop_lon": lon,
            **info_endereco
        })

        # Respeitar limite de uso da API (1 requisição/segundo)
        time.sleep(1)

    df_enderecos = pd.DataFrame(resultados)
    print(f"[info] Endereços obtidos: {len(df_enderecos)}")

    # ------------------------------------------------------------
    # 4) Juntar endereços de volta na tabela de paradas
    # ------------------------------------------------------------
    df_refinado = df.merge(
        df_enderecos,
        on=["stop_lat", "stop_lon"],
        how="left"
    )

    print("[info] Exemplo de paradas refinadas:")
    print(df_refinado.head())

    # ------------------------------------------------------------
    # 5) Salvar resultado em Parquet
    # ------------------------------------------------------------
    PARADAS_SILVER.mkdir(parents=True, exist_ok=True)
    df_refinado.to_parquet(ARQ_PARADAS_ENDERECO, index=False)
    print(f"[ok] Paradas refinadas com endereço salvas em: {ARQ_PARADAS_ENDERECO}")


if __name__ == "__main__":
    main()
 
