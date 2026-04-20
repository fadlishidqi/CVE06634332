import os
from dotenv import load_dotenv
from apps.common.loadProperties import loadProperties
from apps.common.decryptData import decryptData

load_dotenv()

def main():
    pathConfig  = "apps/config/oracleConnection.properties"
    fernet_key  = os.getenv("FERNET_KEY", "")

    if not fernet_key:
        print("ERROR: FERNET_KEY tidak ditemukan di .env")
        return

    print(f"Membaca file properties: {pathConfig}...")
    props = loadProperties(pathConfig=pathConfig)

    if not isinstance(props, dict):
        print("Gagal membaca file properties.")
        return

    print(f"Berhasil membaca {len(props)} properties.")

    encrypted_password = props.get("jdbc.password", "")
    if encrypted_password:
        print("Mendekripsi jdbc.password...")
        decrypted_password = decryptData(key=fernet_key, encryptText=encrypted_password)

        if isinstance(decrypted_password, str):
            props["jdbc.password"] = decrypted_password
            print("jdbc.password berhasil didekripsi.")
        else:
            print("Gagal mendekripsi jdbc.password.")
            return

    print("\nProperties yang berhasil dibaca:")
    for k, v in props.items():
        display_val = "***hidden***" if "password" in k.lower() else v
        print(f"  {k} = {display_val}")

    print("\nProperties siap digunakan untuk koneksi Oracle.")
    return props

if __name__ == "__main__":
    main()