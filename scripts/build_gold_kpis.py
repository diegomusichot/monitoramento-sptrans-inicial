#!/usr/bin/env python
"""
Script: build_gold_kpis.py

Objetivo:
  - Ler o arquivo Silver refinado:
        silver/sptrans_refinado/resumo_linhas_horas.parquet
  - Criar 4 tabelas Gold em Parquet, com KPIs por linha/hora/sentido, salvando em subpastas:
        gold/sptrans/Frota_Ativa_Hora/kpi_frota_ativa_hora.parquet
        gold/sptrans/Intensidade_Telemetria/kpi_intensidade_telemetria_hora.parquet
        gold/sptrans/Cobertura_Operacional/kpi_cobertura_operacional_hora.parquet
        gold/sptrans/Perfil_Operacao_Periodo/kpi_perfil_operacao_periodo.parquet

Observações:
  - Funciona dentro do Docker (Airflow) e localmente.
  - Respeita SPTRANS_GOLD_DIR (ex.: para staging: /opt/airflow/data_lake/gold/_staging/sptrans).
"""

from pathlib import Path
import os
import duckdb

# Detecta ambiente
INSIDE_DOCKER = Path("/opt/airflow").exists()
BASE = Path("/opt/airflow/data_lake") if INSIDE_DOCKER else Path("data_lake")

# Entrada (refinado)
REFINADO = BASE / "silver" / "sptrans_refinado" / "resumo_linhas_horas.parquet"

# Saída base (Gold). Pode ser sobrescrita por variável para usar STAGING.
GOLD_BASE = Path(os.getenv("SPTRANS_GOLD_DIR", (BASE / "gold" / "sptrans")))
GOLD_BASE.mkdir(parents=True, exist_ok=True)

# Subpastas finais pedidas
DIR_FROTA = GOLD_BASE / "Frota_Ativa_Hora"
DIR_INTENSIDADE = GOLD_BASE / "Intensidade_Telemetria"
DIR_COBERTURA = GOLD_BASE / "Cobertura_Operacional"
DIR_PERFIL = GOLD_BASE / "Perfil_Operacao_Periodo"

for d in (DIR_FROTA, DIR_INTENSIDADE, DIR_COBERTURA, DIR_PERFIL):
    d.mkdir(parents=True, exist_ok=True)

def main():
    print(f"[info] REFINADO: {REFINADO.resolve()}")
    print(f"[info] GOLD BASE: {GOLD_BASE.resolve()}")

    if not REFINADO.exists():
        print("[falha] Arquivo resumo_linhas_horas.parquet não encontrado.")
        return

    con = duckdb.connect(database=":memory:")

    # View de origem
    con.execute(f"""
        CREATE OR REPLACE VIEW resumo AS
        SELECT *
        FROM parquet_scan('{REFINADO.as_posix()}');
    """)
    total = con.execute("SELECT COUNT(*) FROM resumo").fetchone()[0]
    print(f"[info] Registros no resumo_linhas_horas: {total}")
    if total == 0:
        print("[gold] Nenhum registro disponível; nada a fazer.")
        return

    # Enriquecimento leve
    con.execute("""
        CREATE OR REPLACE VIEW resumo_enriquecido AS
        SELECT
            dt,
            hh,
            cod_linha,
            letreiro_origem,
            letreiro_destino,
            sentido,
            dia_semana,
            tipo_dia,
            periodo_dia,
            qtd_veiculos_distintos,
            qtd_registros,
            primeira_posicao_local,
            ultima_posicao_local,
            CASE WHEN qtd_veiculos_distintos > 0
                 THEN qtd_registros::DOUBLE / qtd_veiculos_distintos
                 ELSE NULL
            END AS registros_por_veiculo,
            DATE_DIFF('minute', primeira_posicao_local, ultima_posicao_local) AS duracao_operacao_min
        FROM resumo;
    """)

    # KPI 1 — Frota ativa por hora
    print("[gold] Gerando Frota_Ativa_Hora...")
    con.execute("""
        CREATE OR REPLACE TABLE kpi_frota_ativa_hora AS
        SELECT
            dt,
            hh,
            cod_linha,
            sentido,
            letreiro_origem,
            letreiro_destino,
            dia_semana,
            tipo_dia,
            periodo_dia,
            qtd_veiculos_distintos AS frota_ativa
        FROM resumo_enriquecido
        ORDER BY dt, hh, cod_linha, sentido;
    """)
    out1 = DIR_FROTA / "kpi_frota_ativa_hora.parquet"
    con.execute(f"COPY kpi_frota_ativa_hora TO '{out1.as_posix()}' (FORMAT 'parquet');")
    print(f"[ok] Frota_Ativa_Hora salvo em: {out1}")

    # KPI 2 — Intensidade de telemetria
    print("[gold] Gerando Intensidade_Telemetria...")
    con.execute("""
        CREATE OR REPLACE TABLE kpi_intensidade_telemetria_hora AS
        SELECT
            dt,
            hh,
            cod_linha,
            sentido,
            letreiro_origem,
            letreiro_destino,
            dia_semana,
            tipo_dia,
            periodo_dia,
            qtd_veiculos_distintos AS frota_ativa,
            qtd_registros,
            registros_por_veiculo
        FROM resumo_enriquecido
        ORDER BY dt, hh, cod_linha, sentido;
    """)
    out2 = DIR_INTENSIDADE / "kpi_intensidade_telemetria_hora.parquet"
    con.execute(f"COPY kpi_intensidade_telemetria_hora TO '{out2.as_posix()}' (FORMAT 'parquet');")
    print(f"[ok] Intensidade_Telemetria salvo em: {out2}")

    # KPI 3 — Cobertura operacional
    print("[gold] Gerando Cobertura_Operacional...")
    con.execute("""
        CREATE OR REPLACE TABLE kpi_cobertura_operacional_hora AS
        SELECT
            dt,
            hh,
            cod_linha,
            sentido,
            letreiro_origem,
            letreiro_destino,
            dia_semana,
            tipo_dia,
            periodo_dia,
            qtd_veiculos_distintos AS frota_ativa,
            duracao_operacao_min
        FROM resumo_enriquecido
        ORDER BY dt, hh, cod_linha, sentido;
    """)
    out3 = DIR_COBERTURA / "kpi_cobertura_operacional_hora.parquet"
    con.execute(f"COPY kpi_cobertura_operacional_hora TO '{out3.as_posix()}' (FORMAT 'parquet');")
    print(f"[ok] Cobertura_Operacional salvo em: {out3}")

    # KPI 4 — Perfil por período e tipo de dia (agregado)
    print("[gold] Gerando Perfil_Operacao_Periodo...")
    con.execute("""
        CREATE OR REPLACE TABLE kpi_perfil_operacao_periodo AS
        SELECT
            cod_linha,
            sentido,
            letreiro_origem,
            letreiro_destino,
            tipo_dia,
            periodo_dia,
            AVG(qtd_veiculos_distintos) AS frota_media,
            AVG(registros_por_veiculo) AS intensidade_media,
            AVG(duracao_operacao_min) AS duracao_media_min
        FROM resumo_enriquecido
        GROUP BY
            cod_linha, sentido, letreiro_origem, letreiro_destino, tipo_dia, periodo_dia
        ORDER BY cod_linha, sentido, tipo_dia, periodo_dia;
    """)
    out4 = DIR_PERFIL / "kpi_perfil_operacao_periodo.parquet"
    con.execute(f"COPY kpi_perfil_operacao_periodo TO '{out4.as_posix()}' (FORMAT 'parquet');")
    print(f"[ok] Perfil_Operacao_Periodo salvo em: {out4}")

    print("[resumo] Camada Gold criada com 4 KPIs em subpastas.")

if __name__ == "__main__":
    main()
