import logging
import json
from apps.common.callApi import callApi

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

API_URL = "https://aihub.astra-honda.com/predict-carrier"

def main():
    # Simulasi data dari Kafka
    dummy_payload = [
        {
            "t": "1774489518",
            "dcrea": "1774489518",
            "wct": "P9PLA0",
            "technum": "IMM04",
            "param": "NH1-TEMP",
            "nvalue": 5.9,
            "vvalue": "test",
            "category": "C",
            "llow": 2,
            "low": 3,
            "high": 7,
            "hhigh": 10
        },
        {
            "t": "1774489518",
            "dcrea": "1774489518",
            "wct": "P9PLA0",
            "technum": "IMM04",
            "param": "INJ-MAX-PRESSURE",
            "nvalue": 5.9,
            "vvalue": "test",
            "category": "C",
            "llow": 2,
            "low": 3,
            "high": 7,
            "hhigh": 10
        }
    ]

    print(f"Mengirim {len(dummy_payload)} baris data ke API cleansing...")
    print(f"URL: {API_URL}\n")

    result = callApi(
        url=API_URL,
        payload=dummy_payload,
        timeout=30
    )

    if result is None:
        print("API GAGAL. Cek log untuk detail.")
        return

    print(f"\nAPI BERHASIL! Menerima {len(result)} baris hasil cleansing.")
    print("Preview hasil:")
    print(json.dumps(result[:2], indent=2))

if __name__ == "__main__":
    main()