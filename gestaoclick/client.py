"""
Cliente HTTP base para a API do GestãoClick.
Gerencia autenticação, paginação, retry e logging.
"""

import os
import time
import logging
from typing import Optional
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

# Lê de st.secrets (Streamlit Cloud) ou os.getenv (local)
def _get_secret(key: str, default: str = None) -> str:
    try:
        import streamlit as st
        return st.secrets.get(key, os.getenv(key, default))
    except Exception:
        return os.getenv(key, default)

BASE_URL     = _get_secret("GESTAOCLICK_BASE_URL", "https://api.gestaoclick.com")
ACCESS_TOKEN = _get_secret("GESTAOCLICK_ACCESS_TOKEN")
SECRET_TOKEN = _get_secret("GESTAOCLICK_SECRET_TOKEN")

MAX_RETRIES = 3
RETRY_DELAY = 2  # segundos


def _headers() -> dict:
    return {
        "access-token": ACCESS_TOKEN,
        "secret-access-token": SECRET_TOKEN,
        "Content-Type": "application/json",
    }


def _request(method: str, endpoint: str, params: dict = None, data: dict = None, retry: int = 0) -> dict:
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    try:
        response = requests.request(
            method,
            url,
            headers=_headers(),
            params=params,
            json=data,
            timeout=30,
        )
        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 5))
            logger.warning(f"Rate limit atingido. Aguardando {wait}s...")
            time.sleep(wait)
            return _request(method, endpoint, params, data, retry)

        response.raise_for_status()
        return response.json()

    except requests.exceptions.Timeout:
        if retry < MAX_RETRIES:
            logger.warning(f"Timeout em {endpoint}. Tentativa {retry + 1}/{MAX_RETRIES}")
            time.sleep(RETRY_DELAY * (retry + 1))
            return _request(method, endpoint, params, data, retry + 1)
        raise

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP {e.response.status_code} em {endpoint}: {e.response.text[:200]}")
        raise

    except requests.exceptions.ConnectionError:
        if retry < MAX_RETRIES:
            logger.warning(f"Erro de conexão em {endpoint}. Tentativa {retry + 1}/{MAX_RETRIES}")
            time.sleep(RETRY_DELAY * (retry + 1))
            return _request(method, endpoint, params, data, retry + 1)
        raise


def get(endpoint: str, params: dict = None) -> dict:
    return _request("GET", endpoint, params=params)


def get_all(endpoint: str, params: dict = None, limit: int = 100) -> list:
    """Busca todos os registros percorrendo todas as páginas automaticamente."""
    params = dict(params or {})
    params["limite"] = limit
    params["pagina"] = 1

    all_data = []
    total_pages = None

    while True:
        result = _request("GET", endpoint, params=params)
        meta = result.get("meta", {})
        data = result.get("data", [])

        if not data:
            break

        all_data.extend(data)

        if total_pages is None:
            total_pages = meta.get("total_paginas", 1)
            total = meta.get("total_registros", "?")
            logger.info(f"  {endpoint}: {total} registros, {total_pages} páginas")

        current_page = meta.get("pagina_atual", 1)
        next_page = meta.get("proxima_pagina")

        if not next_page or current_page >= total_pages:
            break

        params["pagina"] = next_page
        time.sleep(0.2)  # respeita rate limit

    return all_data


def get_since(endpoint: str, date_field: str, since_date: str, extra_params: dict = None) -> list:
    """Busca registros modificados/criados a partir de uma data."""
    params = dict(extra_params or {})
    params[date_field] = since_date
    return get_all(endpoint, params)
