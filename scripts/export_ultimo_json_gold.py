#!/usr/bin/env python
"""
Script: export_ultimo_json_gold.py

Objetivo:
  - Localizar o arquivo JSON mais recente na camada Bronze
    (bronze/sptrans/posicao/dt=*/hh=*/posicao_*.json)
  - Copiar esse arquivo para a camada Gold em:
    gold/sptrans/Ultimo_JSON/posicao_ultimo.json

Uso (dentro do container Airflow):
  python /opt/airflow/scripts/export_ultimo_json_gold.py
"""

from pathlib import Path
import shutil
import sys
from datetime import datetime

def main():
    # Detecta se está rodando dentro do Docker (Airflow)
    inside_docker = Path("/opt/airflow").exists()
    base = Path("/opt/airflow/data_lake") if inside_docker else Path("data_lake")

    bronze_dir = base / "bronze" / "sptrans" / "posicao"
    gold_dir = base / "gold" / "sptrans" / "Ultimo_JSON"

    print(f"[info] BRONZE: {bronze_dir}")
    print(f"[info] GOLD (Ultimo_JSON): {gold_dir}")

    if not bronze_dir.exists():
        print("[erro] Pasta da Bronze não encontrada.")
        sys.exit(1)

    # Garante a pasta de destino
    gold_dir.mkdir(parents=True, exist_ok=True)

    # Busca todos os JSON da Bronze (dt=*/hh=*/posicao_*.json)
    pattern = "dt=*/hh=*/posicao_*.json"
    arquivos = list(bronze_dir.glob(pattern))

    if not arquivos:
        print("[erro] Nenhum arquivo JSON encontrado na Bronze com padrão:", pattern)
        sys.exit(1)

    # Escolhe o mais recente pelo horário de modificação (mtime)
    ultimo = max(arquivos, key=lambda p: p.stat().st_mtime)

    # Só para log: extrai dt/hh se possível
    try:
        dt_part = ultimo.parent.parent.name   # dt=YYYY-MM-DD
        hh_part = ultimo.parent.name          # hh=HH
    except Exception:
        dt_part = "desconhecido"
        hh_part = "desconhecido"

    mtime = datetime.fromtimestamp(ultimo.stat().st_mtime)

    print(f"[info] Arquivo mais recente encontrado:")
    print(f"       caminho: {ultimo}")
    print(f"       partição: {dt_part}/{hh_part}")
    print(f"       modificado em: {mtime}")

    destino = gold_dir / "posicao_ultimo.json"

    # Copia mantendo metadata básica
    shutil.copy2(ultimo, destino)
    print(f"[ok] Último JSON copiado para: {destino.resolve()}")

if __name__ == "__main__":
    main()

