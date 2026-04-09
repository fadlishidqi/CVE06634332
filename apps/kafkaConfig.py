import sys
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

        print("CONNECTED! Menunggu data...\n")

        for message in consumer:
            print(f"Topic     : {message.topic}")
            print(f"Partition : {message.partition}")
            print(f"Offset    : {message.offset}")
            
            try:
                payload = message.value.decode("utf-8")
            except UnicodeDecodeError:
                payload = str(message.value)
                
            print(f"Value     : {payload}")
            print("-" * 50)

    except KafkaError as e:
        print("GAGAL CONSUMER (Kafka Error)")
        print(str(e))
        sys.exit(1)

    except Exception as e:
        print("GAGAL CONSUMER (Error Sistem)")
        print(str(e))
        sys.exit(1)

if __name__ == "__main__":
    start_consumer()