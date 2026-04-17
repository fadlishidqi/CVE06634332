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
            
        for col in df_new.columns:
            if df_new[col].dtype == 'object':
                df_new[col] = df_new[col].astype(str).tolist()

        new_data_list = json.loads(df_new.to_json(orient='records'))
        
        existing_data = []
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r') as f:
                    existing_data = json.load(f)
            except json.JSONDecodeError:
                existing_data = []
                
        existing_data.extend(new_data_list)
        
        with open(json_path, 'w') as f:
            json.dump(existing_data, f, indent=4)
        
        try:
            if os.path.exists(parquet_path):
                df_existing = pd.read_parquet(parquet_path)
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new
                
            df_combined.to_parquet(parquet_path, engine='pyarrow', index=False)
            
        except Exception as pq_err:
            print(f"\n[INFO] File Parquet lama bermasalah ({pq_err}).")
            print("[INFO] Memaksa menimpa file Parquet dengan data baru...")
            df_new.to_parquet(parquet_path, engine='pyarrow', index=False)
            
        print(f"Data SUKSES tersimpan ke: {json_path} & {parquet_path}")
        return True
    
    except Exception as e:
        print(f"Fatal Error saat append file: {e}")
        return False