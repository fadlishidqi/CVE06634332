import os
from dotenv import load_dotenv
from apps.common.decryptData import decryptData

load_dotenv()

def main():
    key         = os.getenv("FERNET_KEY", "")
    encryptText = os.getenv("ENCRYPTED_TEXT", "")

    if not key:
        print("ERROR: FERNET_KEY tidak ditemukan di .env")
        return

    if not encryptText:
        print("ERROR: ENCRYPTED_TEXT tidak ditemukan di .env")
        return

    print("Mendekripsi data...")
    result = decryptData(key=key, encryptText=encryptText)

    if isinstance(result, str):
        print(f"Berhasil! Plaintext: {result}")
    else:
        print("Gagal mendekripsi data. Periksa FERNET_KEY atau ENCRYPTED_TEXT Anda.")

if __name__ == "__main__":
    main()