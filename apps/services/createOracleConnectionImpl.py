import os
from dotenv import load_dotenv
# from apps.logger import setupLogger
from apps.common.createOracleConnection import createOracleConnection

load_dotenv()
# setupLogger()

def main():
    pathConfig = "apps/config/oracleConnection.properties"
    fernet_key = os.getenv("FERNET_KEY", "")

    if not fernet_key:
        print("ERROR: FERNET_KEY tidak ditemukan di .env")
        return

    print(f"Mencoba koneksi ke Oracle menggunakan: {pathConfig}...")
    conn = createOracleConnection(
        pathConfig=pathConfig,
        fernetKey=fernet_key
    )

    if conn is None:
        print("Koneksi Oracle GAGAL. Cek log untuk detail.")
        return

    print("Koneksi Oracle BERHASIL!")

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT SYSDATE FROM DUAL")
        row = cursor.fetchone()
        print(f"Server time: {row[0]}")
        cursor.close()
    finally:
        conn.close()
        print("Koneksi ditutup.")

if __name__ == "__main__":
    main()