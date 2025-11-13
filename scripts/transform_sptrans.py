#!/usr/bin/env python
"""
Script: transform_sptrans.py

Objetivo:
  - Ler arquivos JSON da camada Bronze da API Olho Vivo (SPTrans)
  - "Achatar" o JSON em uma tabela de posições de veículos
  - Incluir o SENTIDO da viagem (campo 'sl' no JSON) em uma coluna 'sentido'
  - Escrever os dados em Parquet na camada Silver, particionado por:
      dt = data (YYYY-MM-DD)
      hh = hora (00–23)

Este script foi pensado para rodar tanto:
  - DENTRO do container do Airflow, quanto
  - LOCALMENTE (fora do Docker), usando a mesma estrutura de pastas.
"""

from pathlib import Path
import json
from collections import defaultdict

import pandas as pd

# -------------------------------------------------------------------
# 1) Detectar ambiente (Docker vs local) e definir caminhos
# -------------------------------------------------------------------
INSIDE_DOCKER = Path("/opt/airflow").exists()

if INSIDE_DOCKER:
    BASE = Path("/opt/airflow/data_lake")
else:
    BASE = Path("data_lake")

BRONZE = BASE / "bronze" / "sptrans" / "posicao"
SILVER = BASE / "silver" / "sptrans" / "posicao"

print(f"[info] Ambiente Docker? {INSIDE_DOCKER}")
print(f"[info] BRONZE: {BRONZE.resolve()}")
print(f"[info] SILVER: {SILVER.resolve()}")

SILVER.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------------
# 2) Função auxiliar para extrair dt e hh do caminho do arquivo
#    - assumindo estrutura (.../dt=YYYY-MM-DD/hh=HH/arquivo.json)
#    - se não encontrar, usa dt/hh como None (e podemos tratar depois)
# -------------------------------------------------------------------
def extrair_dt_hh(path: Path):
    dt = None
    hh = None
    for part in path.parts:
        if part.startswith("dt="):
            dt = part.split("=", 1)[1]
        if part.startswith("hh="):
            hh = part.split("=", 1)[1]
    return dt, hh


# -------------------------------------------------------------------
# 3) Função para processar UM arquivo JSON da Bronze
#    - Lê o arquivo
#    - Ignora se for "null" ou vazio
#    - Para cada linha (l) e veículo (vs), gera um registro tabular
#    - Inclui o SENTIDO (campo 'sl') em cada linha
# -------------------------------------------------------------------
def processar_arquivo_json(path: Path):
    registros = []

    texto = path.read_text(encoding="utf-8").strip()
    if not texto or texto.lower() == "null":
        # Arquivo vazio ou "null" – ignorar
        return registros

    try:
        data = json.loads(texto)
    except json.JSONDecodeError:
        print(f"[aviso] JSON inválido em {path}, ignorando.")
        return registros

    # dt/hh preferencialmente vêm da estrutura de pastas
    dt, hh = extrair_dt_hh(path)

    # hr no JSON é o horário "texto" da coleta, ex: "23:52"
    hr = data.get("hr")

    # Lista de linhas em 'l'
    for linha in data.get("l", []):
        cod_linha = linha.get("c")        # ex: "5164-21"
        letreiro_origem = linha.get("lt0")
        letreiro_destino = linha.get("lt1")
        sentido = linha.get("sl")         # ⬅ AQUI: SENTIDO DA VIAGEM (1, 2, etc.)

        # Veículos dessa linha em 'vs'
        for v in linha.get("vs", []):
            row = {
                "dt": dt,                        # data (YYYY-MM-DD)
                "hh": hh,                        # hora (00–23)
                "hr": hr,                        # horário da API, ex: "23:52"
                "cod_linha": cod_linha,
                "letreiro_origem": letreiro_origem,
                "letreiro_destino": letreiro_destino,
                "sentido": sentido,              # ⬅ NOVA COLUNA: sentido da viagem
                "prefixo": v.get("p"),           # identificador do veículo
                "acessivel": v.get("a"),         # booleano: acessível ou não
                "ts_posicao": v.get("ta"),       # timestamp ISO da posição
                "lat": v.get("py"),              # latitude
                "lon": v.get("px"),              # longitude
            }
            registros.append(row)

    return registros


# -------------------------------------------------------------------
# 4) Percorrer todos os JSONs da Bronze e acumular registros
#    - Para evitar dataframes gigantes em memória, agrupamos por (dt, hh)
#      e gravamos direto em arquivos Parquet particionados.
# -------------------------------------------------------------------
def main():
    if not BRONZE.exists():
        print("[falha] Pasta Bronze não encontrada. Verifique se já rodou a ingestão.")
        return

    # Vamos acumular registros em um dicionário:
    # chave: (dt, hh) -> lista de dicts
    buckets = defaultdict(list)

    arquivos = sorted(BRONZE.rglob("*.json"))
    print(f"[info] Arquivos JSON encontrados na Bronze: {len(arquivos)}")

    if not arquivos:
        print("[info] Nenhum JSON para processar.")
        return

    for i, path in enumerate(arquivos, start=1):
        regs = processar_arquivo_json(path)
        if not regs:
            continue

        for r in regs:
            dt = r.get("dt")
            hh = r.get("hh")
            if dt is None or hh is None:
                # Se não conseguimos extrair dt/hh da pasta,
                # você pode optar por descartar ou tratar diferente.
                # Aqui, só vamos ignorar.
                continue
            buckets[(dt, hh)].append(r)

        if i % 50 == 0:
            print(f"[info] Processados {i} arquivos JSON...")

    # Agora, para cada (dt, hh), criamos um DataFrame e gravamos Parquet
    total_linhas = 0
    for (dt, hh), rows in buckets.items():
        df = pd.DataFrame(rows)
        total_linhas += len(df)

        # Caminho de saída particionado por dt/hh
        out_dir = SILVER / f"dt={dt}" / f"hh={hh}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "posicao.parquet"

        # Se já existir, podemos concatenar (aqui sobrescrevemos
        # para simplificar, mas você pode mudar se quiser).
        df.to_parquet(out_path, index=False)
        print(f"[ok] Gravado {len(df)} registros em {out_path}")

    print(f"[resumo] Total de registros escritos na Silver: {total_linhas}")


if __name__ == "__main__":
    main()