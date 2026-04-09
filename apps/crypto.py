from cryptography.fernet import Fernet

def decrypt_password(encrypted_password: str, key: str) -> str:
    try:
        cipher_suite = Fernet(key.encode())
        decrypted = cipher_suite.decrypt(encrypted_password.encode())
        return decrypted.decode()
    except Exception as e:
        raise ValueError(f"Gagal, Periksa Key Anda. Error: {e}")