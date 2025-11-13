#!/usr/bin/env python
"""
Ingestão de posições de ônibus da SPTrans (Olho Vivo).
- Detecta ambiente: local (Windows) ou Docker (Airflow)
- Faz login (form-urlencoded ou querystring)
- Obtém dados de posição de ônibus
- Salva em data_lake/bronze/sptrans/posicao/posicao_YYYYMMDD_HHMMSS.json
"""

import os
import json
import time
from datetime import datetime
from pathlib import Path
import requests

# --- Configurações básicas ---
BASE_URL = "http://api.olhovivo.sptrans.com.br/v2.1"
TOKEN = os.getenv("SPTRANS_TOKEN")

# Detecta ambiente e define caminho de saída automaticamente
INSIDE_DOCKER = Path("/opt/airflow").exists()
if INSIDE_DOCKER:
    OUTDIR = Path("/opt/airflow/data_lake/bronze/sptrans/posicao")
else:
    OUTDIR = Path("data_lake/bronze/sptrans/posicao").resolve()

OUTDIR.mkdir(parents=True, exist_ok=True)

from datetime import datetime
# cria partições por data/hora: data_lake/.../dt=YYYY-MM-DD/hh=HH/
now = datetime.now()
OUTDIR = OUTDIR / f"dt={now:%Y-%m-%d}" / f"hh={now:%H}"
OUTDIR.mkdir(parents=True, exist_ok=True)


# --- Função de login com fallback ---
def login(session: requests.Session, token: str) -> None:
    """
    Faz login no Olho Vivo tentando:
    1) POST com token no corpo (form-urlencoded)
    2) POST com token na querystring
    Lança erro detalhado se falhar.
    """
    if not token or token.strip() == "":
        raise RuntimeError("SPTRANS_TOKEN não configurado. Configure o token no .env ou variável de ambiente.")

    base = f"{BASE_URL}/Login/Autenticar"

    # 1) Tenta POST com form-urlencoded
    try:
        r = session.post(base, data={"token": token}, timeout=30)
        if r.status_code == 200 and r.text.strip().lower() == "true":
            print("[login] autenticado via form")
            return
        if r.status_code not in (404, 405):
            r.raise_for_status()
    except requests.HTTPError:
        pass

    # 2) Fallback: querystring
    r = session.post(f"{base}?token={token}", timeout=30)
    if r.status_code == 200 and r.text.strip().lower() == "true":
        print("[login] autenticado via querystring")
        return

    raise RuntimeError(
        f"Falha na autenticação. status={r.status_code}, body={r.text!r}, url={r.request.url}"
    )


# --- Busca dados da posição ---
def fetch_posicao(session: requests.Session) -> dict:
    """
    Obtém a posição dos ônibus.
    """
    url = f"{BASE_URL}/Posicao"
    r = session.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


# --- Função principal ---
def main() -> None:
    print(f"[init] ambiente: {'docker' if INSIDE_DOCKER else 'local'}")
    print(f"[init] salvando em: {OUTDIR}")

    with requests.Session() as s:
        login(s, TOKEN)
        data = fetch_posicao(s)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = OUTDIR / f"posicao_{ts}.json"
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print(f"[ok] salvo em: {outfile.resolve()}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[erro]", e)
        raise