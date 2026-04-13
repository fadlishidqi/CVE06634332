import pandas as pd
import json
import os
from datetime import datetime
from pathlib import Path
import io

def saveAndAppend(json_str: str, base_dir: str = "apps/data/result"):
    try:
        today_date = datetime.now().strftime("%Y-%m-%d")
        
        Path(base_dir).mkdir(parents=True, exist_ok=True)
        json_path = f"{base_dir}/data_{today_date}.json"
        parquet_path = f"{base_dir}/data_{today_date}.parquet"

        df_new = pd.read_json(io.StringIO(json_str))
        
        if df_new.empty:
            return True
            
        # [PERBAIKAN PARQUET] Merapikan memori kolom tipe 'object'
        for col in df_new.columns:
            if df_new[col].dtype == 'object':
                df_new[col] = df_new[col].astype(str).tolist()

        # =========================================================
        # 1. Simpan ke format Standard JSON Array [...]
        # =========================================================
        new_data_list = json.loads(df_new.to_json(orient='records'))
        
        existing_data = []
        if os.path.exists(json_path):
            # Baca data lama jika filenya sudah ada
            try:
                with open(json_path, 'r') as f:
                    existing_data = json.load(f)
            except json.JSONDecodeError:
                existing_data = [] # Jika file korup/kosong, mulai dari awal
                
        # Gabungkan data lama dengan data baru dari batch ini
        existing_data.extend(new_data_list)
        
        # Tulis ulang sebagai Array JSON yang rapi
        with open(json_path, 'w') as f:
            json.dump(existing_data, f, indent=4)
        
        # =========================================================
        # 2. Simpan ke Parquet
        # =========================================================
        try:
            if os.path.exists(parquet_path):
                df_new.to_parquet(parquet_path, engine='pyarrow', append=True)
            else:
                df_new.to_parquet(parquet_path, engine='pyarrow')
        except Exception as pq_err:
            print(f"\n[INFO] File Parquet lama korup/beda skema ({pq_err}).")
            print("[INFO] Memaksa menimpa file Parquet...")
            df_new.to_parquet(parquet_path, engine='pyarrow', append=False)
            
        print(f"Data SUKSES tersimpan ke: {json_path} & {parquet_path}")
        return True
    
    except Exception as e:
        print(f"Fatal Error saat append file: {e}")
        return False