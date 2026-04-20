from influxdb import InfluxDBClient
import logging
from pathlib import Path
from apps.common.loadProperties import loadProperties
from apps.common.decryptData import decryptData

logger = logging.getLogger(__name__)

ALLOWED_DATABASE = "AMAZONE"

def createInfluxConnection(
    pathConfig: str | Path,
    fernetKey: str | None = None
) -> InfluxDBClient | None:

    if not pathConfig:
        logger.error("PathConfig tidak boleh kosong.")
        return None

    props = loadProperties(pathConfig=pathConfig)
    if not isinstance(props, dict):
        logger.error("Gagal membaca file properties InfluxDB.")
        return None

    host     = props.get("influx.host", "")
    port     = props.get("influx.port", "8086")
    username = props.get("influx.username", "")
    password = props.get("influx.password", "")
    database = props.get("influx.database", "")

    if not all([host, port, username, password, database]):
        logger.error("Konfigurasi InfluxDB tidak lengkap. Periksa file properties.")
        return None

    if database != ALLOWED_DATABASE:
        logger.error(f"Akses ditolak! Database '{database}' tidak diizinkan. Hanya '{ALLOWED_DATABASE}' yang boleh diakses.")
        return None

    if fernetKey:
        logger.info("Mendekripsi password InfluxDB...")
        decrypted = decryptData(key=fernetKey, encryptText=password)
        if not isinstance(decrypted, str):
            logger.error("Gagal mendekripsi password InfluxDB.")
            return None
        password = decrypted

    try:
        logger.info(f"Mencoba koneksi ke InfluxDB: {host}:{port} database '{database}'...")

        client = InfluxDBClient(
            host=host,
            port=int(port),
            username=username,
            password=password,
            database=database
        )

        # Validasi koneksi dengan ping
        client.ping()
        logger.info(f"Koneksi InfluxDB BERHASIL! Database aktif: '{database}'")
        return client

    except Exception as e:
        logger.error(f"Gagal koneksi ke InfluxDB: {e}")
        return None