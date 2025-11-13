#!/usr/bin/env python
"""
Script: refinar_sptrans.py
Objetivo:
  - Ler os dados da camada Silver (Parquet) de posições
  - Usar DuckDB para:
      * Criar colunas derivadas (período do dia, data/hora local, dia da semana, tipo de dia)
      * Agregar por data, hora, linha E SENTIDO
  - Salvar o resultado em uma camada Silver refinada (linha/hora/sentido)
"""

from pathlib import Path
import duckdb
import pandas as pd

# Detecta se está dentro do container do Airflow
INSIDE_DOCKER = Path("/opt/airflow").exists()

BASE = Path("/opt/airflow/data_lake") if INSIDE_DOCKER else Path("data_lake")

# Caminho da Silver (entrada)
SILVER = BASE / "silver" / "sptrans" / "posicao"

# Caminho da Silver Refinada (saída)
REFINADO = BASE / "silver" / "sptrans_refinado"
REFINADO.mkdir(parents=True, exist_ok=True)


def main():
    print(f"[info] SILVER: {SILVER.resolve()}")
    print(f"[info] REFINADO: {REFINADO.resolve()}")

    if not SILVER.exists():
        print("[falha] Pasta Silver não encontrada. Rode primeiro o transform_sptrans.py.")
        return

    # ---------------------------------------------------------------
    # 1) Conectar ao DuckDB (banco em memória)
    # ---------------------------------------------------------------
    con = duckdb.connect(database=":memory:")

    # ---------------------------------------------------------------
    # 2) Ler todos os Parquets de posição (Silver)
    # ---------------------------------------------------------------
    parquet_pattern = str(SILVER / "dt=*" / "hh=*" / "posicao.parquet")
    print(f"[info] Lendo arquivos Parquet da Silver: {parquet_pattern}")

    con.execute(f"""
        CREATE OR REPLACE VIEW posicao AS
        SELECT * FROM parquet_scan('{parquet_pattern}');
    """)

    total = con.execute("SELECT COUNT(*) FROM posicao").fetchone()[0]
    print(f"[info] Registros carregados da Silver (posicao): {total}")

    if total == 0:
        print("[refino] Nenhum dado disponível na Silver.")
        return

    # ---------------------------------------------------------------
    # 3) Criar view com colunas derivadas
    #    - ts_utc: ts_posicao convertido para TIMESTAMP
    #    - ts_local: ts_utc ajustado para horário de São Paulo (UTC-3)
    #    - minuto_local: HH:MM da hora local
    #    - dia_semana: nome do dia (Segunda, Terça, ...)
    #    - tipo_dia: Dia útil / Sábado / Domingo
    #    - periodo_dia: Manhã / Tarde / Noite / Madrugada
    #    - sentido: trazido da Silver (1, 2, etc.)
    # ---------------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW posicao_refinada AS
        SELECT
            dt,
            hh,
            cod_linha,
            letreiro_origem,
            letreiro_destino,
            sentido,                        -- SENTIDO da linha (sl)
            prefixo,
            acessivel,
            ts_posicao,
            lat,
            lon,
            -- Converte o texto ts_posicao (VARCHAR) para TIMESTAMP
            CAST(ts_posicao AS TIMESTAMP) AS ts_utc,
            -- Ajuste de fuso horário: a API retorna UTC, aqui trazemos para horário de São Paulo (UTC-3)
            CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR AS ts_local,
            -- Minuto local (HH:MM) para possíveis análises mais granulares
            strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%H:%M') AS minuto_local,
            -- Dia da semana numérico (0=Domingo,...,6=Sábado)
            strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') AS dia_semana_num,
            CASE
                WHEN strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') = '0' THEN 'Domingo'
                WHEN strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') = '1' THEN 'Segunda'
                WHEN strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') = '2' THEN 'Terça'
                WHEN strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') = '3' THEN 'Quarta'
                WHEN strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') = '4' THEN 'Quinta'
                WHEN strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') = '5' THEN 'Sexta'
                WHEN strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') = '6' THEN 'Sábado'
                ELSE 'Desconhecido'
            END AS dia_semana,
            CASE
                WHEN strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') IN ('1','2','3','4','5') THEN 'Dia útil'
                WHEN strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') = '6' THEN 'Sábado'
                WHEN strftime(CAST(ts_posicao AS TIMESTAMP) - INTERVAL 3 HOUR, '%w') = '0' THEN 'Domingo'
                ELSE 'Desconhecido'
            END AS tipo_dia,
            CASE
                WHEN CAST(hh AS INT) BETWEEN 5 AND 11 THEN 'Manhã'
                WHEN CAST(hh AS INT) BETWEEN 12 AND 17 THEN 'Tarde'
                WHEN CAST(hh AS INT) BETWEEN 18 AND 23 THEN 'Noite'
                ELSE 'Madrugada'
            END AS periodo_dia
        FROM posicao;
    """)

    # ---------------------------------------------------------------
    # 4) Agregar dados por dt, hh, linha, SENTIDO, dia_semana, tipo_dia, periodo_dia
    # ---------------------------------------------------------------
    query = """
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
            COUNT(DISTINCT prefixo) AS qtd_veiculos_distintos,
            COUNT(*) AS qtd_registros,
            MIN(ts_local) AS primeira_posicao_local,
            MAX(ts_local) AS ultima_posicao_local
        FROM posicao_refinada
        GROUP BY
            dt,
            hh,
            cod_linha,
            letreiro_origem,
            letreiro_destino,
            sentido,
            dia_semana,
            tipo_dia,
            periodo_dia
        ORDER BY
            dt,
            hh,
            cod_linha,
            sentido;
    """

    resultado = con.execute(query).fetch_df()
    print(f"[info] Registros na tabela refinada: {len(resultado)}")

    # ---------------------------------------------------------------
    # 5) Salvar resultado em Parquet
    # ---------------------------------------------------------------
    out_path = REFINADO / "resumo_linhas_horas.parquet"
    resultado.to_parquet(out_path, index=False)
    print(f"[refino] Arquivo refinado salvo em: {out_path}")


if __name__ == "__main__":
    main()
