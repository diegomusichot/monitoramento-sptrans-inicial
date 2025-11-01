#!/usr/bin/env python
"""
Ingestão de posições de ônibus da SPTrans (Olho Vivo) e salvamento na camada Bronze.
Fluxo:
1) Autentica na API (gera cookie de sessão)
2) Chama /Posicao
3) Salva o JSON em data_lake/bronze/sptrans/posicao/posicao_YYYYMMDD_HHMMSS.json
"""

import os
import json
from datetime import datetime
from pathlib import Path

import requests

# --- Configurações ---
BASE_URL = "http://api.olhovivo.sptrans.com.br/v2.1"  # API oficial da SPTrans (HTTP mesmo)
TOKEN = os.getenv("SPTRANS_TOKEN")  # lido do ambiente (vem do .env via docker compose)
OUTDIR = Path("/opt/airflow/data_lake/bronze/sptrans/posicao")  # caminho visto de dentro do container


def login(session: requests.Session, token: str) -> None:
    """
    Faz login no Olho Vivo tentando os dois formatos comuns:
    1) POST com token no corpo (form-urlencoded)
    2) POST com token na querystring
    Levanta um erro claro se não autenticar.
    """
    if not token or token.startswith("coloque_"):
        raise RuntimeError("SPTRANS_TOKEN não configurado. Edite o arquivo .env.")

    base = f"{BASE_URL}/Login/Autenticar"

    # 1) Tenta POST com form-urlencoded
    try:
        r = session.post(
            base,
            data={"token": token},
            headers={"Content-Type": "application/x-www-form-urlencoded",
                     "User-Agent": "airflow-sptrans/1.0"},
            timeout=30,
            allow_redirects=True,
        )
        if r.status_code == 200 and r.text.strip().lower() == "true":
            return
        # Se veio 404 aqui, alguns ambientes só aceitam querystring
        if r.status_code not in (404, 405):
            r.raise_for_status()
    except requests.HTTPError:
        # cai pro fallback abaixo
        pass

    # 2) Fallback: POST com token na querystring
    r = session.post(
        f"{base}?token={token}",
        headers={"User-Agent": "airflow-sptrans/1.0"},
        timeout=30,
        allow_redirects=True,
    )
    if r.status_code == 200 and r.text.strip().lower() == "true":
        return

    # Se chegou aqui, falhou nos dois formatos: exponha diagnóstico
    raise RuntimeError(
        f"Falha na autenticação Olho Vivo. "
        f"status={r.status_code}, body={r.text!r}, url={r.request.url}"
    )



def fetch_posicao(session: requests.Session) -> dict:
    """
    Chama o endpoint /Posicao e retorna o JSON com a posição dos ônibus.
    """
    url = f"{BASE_URL}/Posicao"
    r = session.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def main() -> None:
    # Garante que a pasta de saída exista (no container)
    OUTDIR.mkdir(parents=True, exist_ok=True)

    # Autentica e busca os dados
    with requests.Session() as s:
        login(s, TOKEN)
        data = fetch_posicao(s)

    # Cria nome de arquivo com timestamp local
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = OUTDIR / f"posicao_{ts}.json"

    # Salva o JSON com UTF-8
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print(f"[OK] Salvo: {outfile}")


if __name__ == "__main__":
    main()

