import requests
import logging
import json
from typing import Any

logger = logging.getLogger(__name__)

def callApi(
    url: str,
    payload: list | str,
    headers: dict | None = None,
    timeout: int = 30
) -> list | None:

    if not url:
        logger.error("URL tidak boleh kosong.")
        return None

    if not payload:
        logger.error("Payload tidak boleh kosong.")
        return None

    try:
        if isinstance(payload, str):
            payload = json.loads(payload.strip())

        if not isinstance(payload, list):
            logger.error("Payload harus berupa list of dict.")
            return None

        default_headers = {"Content-Type": "application/json"}
        if headers:
            default_headers.update(headers)

        logger.info(f"Hitting API: {url}")
        logger.info(f"Jumlah data dikirim: {len(payload)} baris")

        response = requests.post(
            url=url,
            json=payload,
            headers=default_headers,
            timeout=timeout
        )

        response.raise_for_status()

        result = response.json()

        if isinstance(result, list):
            logger.info(f"API berhasil. Menerima {len(result)} baris hasil cleansing.")
            return result
        elif isinstance(result, dict):
            for key in ["data", "result", "results", "output"]:
                if key in result and isinstance(result[key], list):
                    logger.info(f"API berhasil. Menerima {len(result[key])} baris dari key '{key}'.")
                    return result[key]
            logger.warning("Response API berbentuk dict tapi tidak ada key data yang dikenal. Wrap sebagai list.")
            return [result]
        else:
            logger.error(f"Format response tidak dikenal: {type(result)}")
            return None

    except requests.exceptions.Timeout:
        logger.error(f"Request timeout setelah {timeout} detik.")
        return None
    except requests.exceptions.ConnectionError:
        logger.error(f"Gagal koneksi ke API: {url}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        return None
    except json.JSONDecodeError:
        logger.error("Gagal parse response API sebagai JSON.")
        return None
    except Exception as e:
        logger.error(f"Error tidak terduga saat hit API: {e}")
        return None