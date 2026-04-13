import sys
import json
import time
import os
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from apps.config import (
    KAFKA_TOPIC,
    KAFKA_SERVERS,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    KAFKA_SSL_CAFILE,
    KAFKA_GROUP_ID
)

# Konfigurasi Output
SAVE_INTERVAL_SECONDS = 60  # Waktu simpan per batch (dalam detik)
OUTPUT_DIR = "apps/data/raw_dump"

def start_consumer():
    try:
        print("Mencoba koneksi Consumer ke Kafka...")
        
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=KAFKA_SASL_USERNAME,
            sasl_plain_password=KAFKA_SASL_PASSWORD,
            ssl_cafile=KAFKA_SSL_CAFILE,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=KAFKA_GROUP_ID
        )

        # Buat folder jika belum ada
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        print("CONNECTED!")
        print(f"🚀 Raw Consumer Berjalan... Menyimpan data setiap {SAVE_INTERVAL_SECONDS} detik.")
        print(f"Folder Output: {OUTPUT_DIR}/\n")

        batch_buffer = []
        start_time = time.time()

        while True:
            # Menggunakan poll agar loop tidak memblokir timer jika data kosong
            messages = consumer.poll(timeout_ms=1000)

            for tp, msgs in messages.items():
                for message in msgs:
                    try:
                        # 1. Decode byte menjadi string
                        payload = message.value.decode("utf-8")
                        
                        # 2. Parse string menjadi JSON
                        val = json.loads(payload)

                        # 3. Bongkar Array NIFI agar data di file JSON rapi
                        if isinstance(val, list):
                            batch_buffer.extend(val)
                        elif isinstance(val, dict):
                            batch_buffer.append(val)
                        elif isinstance(val, str):
                            # Jika terkena double-encoding dari NIFI
                            parsed_val = json.loads(val)
                            if isinstance(parsed_val, list):
                                batch_buffer.extend(parsed_val)
                            else:
                                batch_buffer.append(parsed_val)
                                
                    except UnicodeDecodeError:
                        print("Peringatan: Gagal melakukan decode data (Bukan UTF-8)")
                    except json.JSONDecodeError:
                        print("Peringatan: Data yang diterima bukan format JSON yang valid")

            # Logika Timer
            current_time = time.time()
            elapsed_time = current_time - start_time

            if elapsed_time >= SAVE_INTERVAL_SECONDS:
                if batch_buffer:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"{OUTPUT_DIR}/raw_data_{timestamp}.json"
                    
                    # Simpan data
                    with open(filename, 'w') as f:
                        json.dump(batch_buffer, f, indent=4)
                    
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] BERHASIL: Menyimpan {len(batch_buffer)} baris data ke {filename}")
                    
                    # Kosongkan memory setelah simpan
                    batch_buffer = []
                else:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Menunggu data... (Tidak ada data dalam {SAVE_INTERVAL_SECONDS} detik terakhir)")
                
                # Reset Timer
                start_time = time.time()

    except KeyboardInterrupt:
        print("\nRaw Consumer dihentikan manual oleh user.")
        # Pengaman: Simpan sisa data saat di-Ctrl+C
        if 'batch_buffer' in locals() and batch_buffer:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{OUTPUT_DIR}/raw_data_sisa_{timestamp}.json"
            with open(filename, 'w') as f:
                json.dump(batch_buffer, f, indent=4)
            print(f"Menyimpan {len(batch_buffer)} sisa data terakhir ke {filename}")
        sys.exit(0)

    except KafkaError as e:
        print("GAGAL CONSUMER (Kafka Error)")
        print(str(e))
        sys.exit(1)

    except Exception as e:
        print("GAGAL CONSUMER (Error Sistem)")
        print(str(e))
        sys.exit(1)
        
    finally:
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    start_consumer()