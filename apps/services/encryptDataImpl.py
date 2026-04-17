import os
from dotenv import load_dotenv
from apps.common.encryptData import encryptData

load_dotenv()

def main():
    key       = os.getenv("FERNET_KEY", "")
    plainText = os.getenv("PLAIN_TEXT", "")

    if not key:
        print("ERROR: FERNET_KEY tidak ditemukan di .env")
        return

    if not plainText:
        print("ERROR: PLAIN_TEXT tidak ditemukan di .env")
        return

    print("Mengenkripsi password...")
    result = encryptData(key=key, plainText=plainText)

    if isinstance(result, str):
        print(f"Hasil Enkripsi = {result}")
    else:
        print("Gagal mengenkripsi data. Periksa FERNET_KEY Anda.")

if __name__ == "__main__":
    main()