#!/usr/bin/env python
"""
Script: carregar_gtfs_planejamento.py
Objetivo:
  - Ler trips.txt, stop_times.txt e calendar.txt do GTFS
  - Derivar, para cada viagem (trip_id), um horário de partida (primeiro horário da viagem)
  - Classificar a viagem por:
      * linha (route_id / route_short_name)
      * direção (direction_id)
      * hora do dia (hh)
      * tipo de dia (Dia útil / Sábado / Domingo / Outro)
  - Agregar em nível linha + direção + tipo de dia + hora:
      * quantidade de viagens planejadas
      * headway médio aproximado (minutos) na hora
  - Salvar em Parquet na camada Silver GTFS
"""

from pathlib import Path
import pandas as pd

# Detecta se está rodando dentro do container
INSIDE_DOCKER = Path("/opt/airflow").exists()
BASE = Path("/opt/airflow/data_lake") if INSIDE_DOCKER else Path("data_lake")

GTFS_DIR = BASE / "reference" / "gtfs"
GTFS_SILVER = BASE / "silver" / "sptrans_gtfs"
GTFS_SILVER.mkdir(parents=True, exist_ok=True)

ARQ_ROTAS = GTFS_SILVER / "rotas_gtfs.parquet"
ARQ_PLANEJAMENTO = GTFS_SILVER / "planejamento_linha_hora.parquet"


def classificar_tipo_dia(row):
    """
    Recebe uma linha do calendar.txt e devolve:
      - 'Dia útil' se segunda a sexta = 1 e sábado/domingo = 0
      - 'Sábado' se só sábado = 1
      - 'Domingo' se só domingo = 1
      - 'Outro' para combinações diferentes (ex.: feriados, serviços especiais)
    """
    mon = row.get("monday", 0)
    tue = row.get("tuesday", 0)
    wed = row.get("wednesday", 0)
    thu = row.get("thursday", 0)
    fri = row.get("friday", 0)
    sat = row.get("saturday", 0)
    sun = row.get("sunday", 0)

    if mon == tue == wed == thu == fri == 1 and sat == 0 and sun == 0:
        return "Dia útil"
    if sat == 1 and mon == tue == wed == thu == fri == sun == 0:
        return "Sábado"
    if sun == 1 and mon == tue == wed == thu == fri == sat == 0:
        return "Domingo"
    return "Outro"


def extrair_hora(departure_time: str) -> int | None:
    """
    Recebe um horário GTFS no formato HH:MM:SS (podendo ser HH >= 24)
    Retorna a hora (0-23) para análise agregada.
    Se inválido, retorna None.
    """
    try:
        partes = str(departure_time).split(":")
        if len(partes) < 2:
            return None
        h = int(partes[0])
        # GTFS permite horas >= 24 (passando da meia-noite)
        return h % 24
    except Exception:
        return None


def main():
    print(f"[info] GTFS_DIR: {GTFS_DIR.resolve()}")
    print(f"[info] GTFS_SILVER: {GTFS_SILVER.resolve()}")

    trips_path = GTFS_DIR / "trips.txt"
    stop_times_path = GTFS_DIR / "stop_times.txt"
    calendar_path = GTFS_DIR / "calendar.txt"

    if not trips_path.exists() or not stop_times_path.exists():
        print("[falha] trips.txt ou stop_times.txt não encontrados. Verifique o GTFS em data_lake/reference/gtfs.")
        return

    # 1) Ler trips.txt
    trips = pd.read_csv(trips_path)
    print(f"[info] trips.txt lido. Linhas: {len(trips)}")

    # 2) Ler stop_times.txt
    stop_times = pd.read_csv(stop_times_path)
    print(f"[info] stop_times.txt lido. Linhas: {len(stop_times)}")

    # 3) Ler calendar.txt (se existir) e classificar tipo de dia por service_id
    if calendar_path.exists():
        calendar = pd.read_csv(calendar_path)
        print(f"[info] calendar.txt lido. Linhas: {len(calendar)}")

        calendar["tipo_dia"] = calendar.apply(classificar_tipo_dia, axis=1)
        service_tipo = calendar[["service_id", "tipo_dia"]].copy()
    else:
        print("[aviso] calendar.txt não encontrado. Todos os serviços serão marcados como 'Outro'.")
        service_tipo = trips[["service_id"]].drop_duplicates().copy()
        service_tipo["tipo_dia"] = "Outro"

    # 4) Determinar horário de partida de cada viagem (primeiro horário da trip_id)
    #    - Para cada trip_id, pegar o menor stop_sequence (primeira parada) e usar o departure_time
    stop_times_first = (
        stop_times.sort_values(["trip_id", "stop_sequence"])
        .groupby("trip_id", as_index=False)
        .first()[["trip_id", "departure_time"]]
    )

    # 5) Juntar viagens (trips) com horário de partida (stop_times_first)
    viagens = trips.merge(stop_times_first, on="trip_id", how="left")
    print(f"[info] Viagens após join com stop_times_first: {len(viagens)}")

    # 6) Juntar com tipo de dia (service_id -> tipo_dia)
    viagens = viagens.merge(service_tipo, on="service_id", how="left")

    # 7) Calcular hora do dia (hh) da departure_time
    viagens["hh"] = viagens["departure_time"].apply(extrair_hora)

    # Remover viagens sem hora interpretável
    antes = len(viagens)
    viagens = viagens.dropna(subset=["hh"])
    depois = len(viagens)
    print(f"[info] Viagens com hora válida: {depois} (removidas {antes - depois} sem horário válido)")

    # Converter hh para inteiro
    viagens["hh"] = viagens["hh"].astype(int)

    # 8) Opcional: juntar com rotas_gtfs.parquet para trazer short_name/long_name
    if ARQ_ROTAS.exists():
        rotas = pd.read_parquet(ARQ_ROTAS)
        print(f"[info] rotas_gtfs.parquet lido. Linhas: {len(rotas)}")

        # Mantém as colunas-chave de rota
        colunas_rotas = ["route_id", "route_short_name", "route_long_name", "route_type"]
        colunas_rotas = [c for c in colunas_rotas if c in rotas.columns]

        viagens = viagens.merge(
            rotas[colunas_rotas],
            on="route_id",
            how="left"
        )
    else:
        print("[aviso] rotas_gtfs.parquet não encontrado. Planejamento será gerado sem nomes de linha.")
        viagens["route_short_name"] = None
        viagens["route_long_name"] = None
        viagens["route_type"] = None

    # 9) Agregar por linha + direção + tipo de dia + hora (hh)
    agrupado = (
        viagens.groupby(
            [
                "route_id",
                "route_short_name",
                "route_long_name",
                "route_type",
                "direction_id",
                "tipo_dia",
                "hh",
            ],
            dropna=False
        )
        .size()
        .reset_index(name="qtd_viagens_planejadas")
    )

    # 10) Headway médio aproximado (minutos) na hora
    #     Se há N viagens numa hora, headway aproximado = 60 / N
    agrupado["headway_medio_min"] = 60.0 / agrupado["qtd_viagens_planejadas"]

    print(f"[info] Linhas na tabela planejamento_linha_hora: {len(agrupado)}")

    # 11) Salvar em Parquet
    agrupado.to_parquet(ARQ_PLANEJAMENTO, index=False)
    print(f"[ok] Planejamento por linha/hora salvo em: {ARQ_PLANEJAMENTO}")


if __name__ == "__main__":
    main()
