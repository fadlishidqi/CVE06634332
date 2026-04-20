import os
import logging
from influxdb.resultset import ResultSet
from dotenv import load_dotenv
from apps.common.createInfluxConnection import createInfluxConnection

load_dotenv()
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def main():
    pathConfig = "apps/config/influxConnection.properties"
    fernet_key = os.getenv("FERNET_KEY", "")

    if not fernet_key:
        print("ERROR: FERNET_KEY tidak ditemukan di .env")
        return

    client = createInfluxConnection(
        pathConfig=pathConfig,
        fernetKey=fernet_key
    )

    if client is None:
        print("Koneksi InfluxDB GAGAL. Cek log untuk detail.")
        return

    print("Koneksi InfluxDB BERHASIL!")

    try:
        result: ResultSet = client.query("SHOW MEASUREMENTS LIMIT 5")  # type: ignore
        measurements = list(result.get_points())
        print(f"Sample measurements: {measurements}")
    finally:
        client.close()
        print("Koneksi ditutup.")

if __name__ == "__main__":
    main()