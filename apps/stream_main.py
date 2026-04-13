import json
import time
import os
import io
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer

from apps.config import (
    KAFKA_TOPIC, KAFKA_SERVERS, KAFKA_SASL_USERNAME, 
    KAFKA_SASL_PASSWORD, KAFKA_SSL_CAFILE, KAFKA_GROUP_ID
)

# Import Common Lengkap (Termasuk saveAppender Anda!)
from apps.common.renameJsonParam import renameJsonParam
from apps.common.aggregateTime import aggregateTime
from apps.common.filterTime import filterTime
from apps.common.replaceData import replaceData
from apps.common.compareValue import compareValue
from apps.common.filterRange import filterRange
from apps.common.normalization import normalization
from apps.common.outlier import outlier
from apps.common.pivotTable import pivotTable
from apps.common.forwardFill import forwardFill
from apps.common.saveAppender import saveAndAppend

BATCH_INTERVAL_SECONDS = 5 * 60  # Interval Batch: 5 Menit


def run_cleansing_pipeline(batch_data: list, historical_state: pd.DataFrame | None):
    if not batch_data:
        return historical_state

    # =================================================================
    # --- 1. PRE-PROCESSING: PENYELARASAN WAKTU MENJADI EPOCH UTC+0 ---
    # =================================================================
    valid_batch = []
    for row in batch_data:
        if 't' in row and 'dcrea' in row:
            if isinstance(row['t'], (int, float)):
                try:
                    # Buang milidetik berlebih
                    row['t'] = int(row['t'] // 1000) * 1000

                    # Konversi ke UTC+0
                    dt_naive = pd.to_datetime(row['dcrea'])
                    dt_utc = dt_naive - pd.Timedelta(hours=7)
                    dcrea_epoch = int(dt_utc.timestamp() * 1000)
                    row['dcrea'] = dcrea_epoch
                    
                    # [HAPUS COMMENT DI BAWAH JIKA INGIN VALIDASI GAP 1 JAM]
                    # if abs(dcrea_epoch - row['t']) <= 3600000:
                    #     valid_batch.append(row)
                    
                    valid_batch.append(row)
                        
                except Exception:
                    pass

    if not valid_batch:
        return historical_state

    json_str = json.dumps(valid_batch)
    preset_path = "apps/preset/preset.json"
    
    with open(preset_path, "r") as f:
        preset = json.load(f)

    # --- BACA GLOBAL SETTINGS DARI PRESET.JSON ---
    do_normalization = False
    do_outlier = False
    for item in preset.get("global_settings", []):
        if item.get("key") == "normalization" and str(item.get("value")).lower() == "true":
            do_normalization = True
        if item.get("key") == "outlier" and str(item.get("value")).lower() == "true":
            do_outlier = True

    print("\n--- Memulai Pipeline Cleansing Batch ---")
    
    keySource_rename = ["vwctid", "vmachineid", "vparam", "catg",  "nllow", "nlow", "nhigh", "nhhigh"]
    keyTarget_rename = ["wct", "technum", "param", "category", "llow", "low", "high", "hhigh"]

    try:
        json_str = renameJsonParam(pathSource=json_str, pathTarget=None, keySource=keySource_rename, keyTarget=keyTarget_rename)
        if not isinstance(json_str, str): raise ValueError("renameJsonParam gagal")
        
        # json_str = aggregateTime(
        #     pathSource=json_str, pathTarget=None, keyTime="t", 
        #     keyValues = ["nvalue", "llow", "low", "vvalue", "nhhigh", "nhigh", "nllow", "nlow", "catg"], 
        #     aggMethods = ["mean", "last", "last", "last", "last", "last", "last", "last", "last"],
        #     keyGroups=["wct", "technum", "param"]
        # )
        # if not isinstance(json_str, str): raise ValueError("aggregateTime gagal")

        # json_str = filterTime(pathSource=json_str, pathTarget=None, keyStart="t", keyEnd="dcrea", maxDelta=1, unitDelta="hours")
        # if not isinstance(json_str, str): raise ValueError("filterTime gagal")
        
        # if json_str.strip() in ["[]", "{}"]:
        #     print("Info: Semua data diabaikan karena batas waktu (filterTime).")
        #     return historical_state

        json_str = replaceData(pathSource=json_str, pathTarget=None, keySource=["category"], fromData=["C", "NC"], toData=[1, 0])
        if not isinstance(json_str, str): raise ValueError("replaceData gagal")
        
        json_str = compareValue(pathSource=json_str, pathTarget=None, keySource="nvalue", keyCompare=["llow", "hhigh", "low", "high"], operators=["<", ">", "<", ">"], resultCompare=[-2, 2, -1, 1], defaultValue=0, keyResult="threshold")
        if not isinstance(json_str, str): raise ValueError("compareValue gagal")
        
        json_str = filterRange(pathSource=json_str, pathTarget=None, pathPreset=preset_path, keyValue="nvalue", keyWct="wct", keyTech="technum", keyParam="param")
        if not isinstance(json_str, str): raise ValueError("filterRange gagal")

        if json_str.strip() in ["[]", "{}"]:
            print("Info: Semua data dihapus karena nilainya di luar batas Normal (Outlier). Batch dilewati.")
            return historical_state

        # =================================================================
        # --- EKSEKUSI OUTLIER & NORMALIZATION (Berdasarkan preset.json) ---
        # =================================================================
        if do_outlier:
            json_str = outlier(
                pathSource=json_str, pathTarget=None, pathPreset=preset_path,
                groupCols=["wct", "technum", "param"], keyValue="nvalue"
            )
            if not isinstance(json_str, str): raise ValueError("outlier gagal")

        if do_normalization:
            json_str = normalization(
                pathSource=json_str, pathTarget=None, pathPreset=preset_path,
                keyValue="nvalue", keyWct="wct", keyTech="technum", keyParam="param"
            )
            if not isinstance(json_str, str): raise ValueError("normalization gagal")
        # =================================================================

        json_str = pivotTable(pathSource=json_str, pathTarget=None, pathPreset=preset_path, indexCols=["t", "wct", "technum"], pivotCol="param", timeCol="t")
        if not isinstance(json_str, str): raise ValueError("pivotTable gagal")

        print("Mempersiapkan Historical Data untuk Forward Fill...")
        
        df_pivot = pd.read_json(io.StringIO(json_str))
        historical_len = 0
        if historical_state is not None and not historical_state.empty:
            historical_len = len(historical_state)
            df_pivot = pd.concat([historical_state, df_pivot], ignore_index=True)
            
        json_str_for_ffill = df_pivot.to_json(orient="records")

        ffill_result = forwardFill(
            pathSource=json_str_for_ffill,
            pathTarget=None,
            pathPreset=preset_path,
            sortCols=["t"],
            groupCols=["wct", "technum"],
            dropNulls=False 
        )

        if not isinstance(ffill_result, str):
            raise ValueError("forwardFill gagal memproses data")

        df_ffilled = pd.read_json(io.StringIO(ffill_result))
        
        if historical_len > 0:
            df_ffilled = df_ffilled.iloc[historical_len:].reset_index(drop=True)

        valid_group_cols = [c for c in ["wct", "technum"] if c in df_ffilled.columns]
        if valid_group_cols:
            next_historical_state = df_ffilled.groupby(valid_group_cols).tail(1)
        else:
            next_historical_state = df_ffilled.tail(1)

        # Ubah DataFrame final menjadi String JSON
        final_json_str = df_ffilled.to_json(orient="records")

        # =================================================================
        # KEMBALI MENGGUNAKAN FUNGSI COMMON ANDA (Lebih aman!)
        # =================================================================
        print("Menyimpan dan melakukan append data ke file...")
        save_status = saveAndAppend(final_json_str) 
        if not save_status:
            print("Peringatan: saveAndAppend mengembalikan nilai False/Gagal.")
        # =================================================================
        
        print("--- Pipeline Batch Selesai ---\n")
        return next_historical_state

    except Exception as e:
        print(f"Error pada Pipeline Batch: {str(e)}")
        return historical_state


def start_stream():
    try:
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
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
    except Exception as e:
        print(f"Gagal koneksi ke Kafka: {e}")
        return

    print(f"🚀 Stream Berjalan... Menunggu data dari Kafka (Batch per {BATCH_INTERVAL_SECONDS/60} menit)\n")
    
    batch_buffer = []
    start_time = time.time()
    historical_state = pd.DataFrame()

    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)
            
            for tp, msgs in messages.items():
                for msg in msgs:
                    val = msg.value
                    
                    if isinstance(val, list):
                        batch_buffer.extend(val)
                    elif isinstance(val, dict):
                        batch_buffer.append(val)
                    elif isinstance(val, str):
                        try:
                            parsed_val = json.loads(val)
                            if isinstance(parsed_val, list):
                                batch_buffer.extend(parsed_val)
                            else:
                                batch_buffer.append(parsed_val)
                        except json.JSONDecodeError:
                            continue
            
            current_time = time.time()
            elapsed_time = current_time - start_time
            
            if elapsed_time >= BATCH_INTERVAL_SECONDS:
                if batch_buffer:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Memproses {len(batch_buffer)} baris data...")
                    historical_state = run_cleansing_pipeline(batch_buffer, historical_state)
                    batch_buffer = []
                else:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Tidak ada data masuk dalam 5 menit terakhir.")
                
                start_time = time.time()

    except KeyboardInterrupt:
        print("\nStream dihentikan manual oleh user.")
    except Exception as e:
        print(f"Fatal Error pada Consumer: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_stream()