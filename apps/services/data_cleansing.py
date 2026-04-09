import pandas as pd
import numpy as np
import json
from pathlib import Path
import logging

from apps.common.parquet_exporter import export_to_parquet
from apps.common.json_exporter import export_to_json

logger = logging.getLogger(__name__)

def apply_cleansing_rules(base_input_path: Path | str, base_output_path: Path | str) -> bool:
    input_path = Path(str(base_input_path) + ".parquet")
    
    preset_path = Path(__file__).parent.parent/'preset'/'preset.json'
    
    if not input_path.exists():
        logger.error(f"File input tidak ditemukan: {input_path}")
        return False

    try:
        with open(preset_path, 'r') as f:
            preset_data = json.load(f)
        preset_params = preset_data.get('parameters', {})
    except Exception as e:
        logger.error(f"Gagal membaca preset.json: {e}")
        return False

    try:
        df = pd.read_parquet(input_path)
    except Exception as e:
        logger.error(f"Gagal membaca file parquet: {e}")
        return False

    try:
        # Konversi waktu ke tipe datetime
        df['time_dt'] = pd.to_datetime(df['time'], format='ISO8601', errors='coerce')
        if df['time_dt'].dt.tz is not None:
            df['time_dt'] = df['time_dt'].dt.tz_localize(None)
        
        df['dcrea_dt'] = pd.to_datetime(df['dcrea'], errors='coerce')
        df['dmodi_dt'] = pd.to_datetime(df['dmodi'], errors='coerce')
        
        df = df.dropna(subset=['time_dt', 'dcrea_dt'])

        df['time'] = df['time_dt'].dt.strftime('%d-%m-%Y %H:%M:%S')
        df['dcrea'] = df['dcrea_dt'].dt.strftime('%d-%m-%Y %H:%M:%S')
        df['dmodi'] = df['dmodi_dt'].dt.strftime('%d-%m-%Y %H:%M:%S')

        # Filter time <= dcrea & max 1 jam selisih
        cond_not_future = df['time_dt'] <= df['dcrea_dt']
        cond_max_1_hour = (df['dcrea_dt'] - df['time_dt']) <= pd.Timedelta(hours=1)
        df = df[cond_not_future & cond_max_1_hour].copy()

        # Flagging CATG
        df['catg'] = df['catg'].map({'C': 1, 'NC': 0}).fillna(-1).astype(int)

        # Flagging threshold
        conditions = [
            (df['nvalue'] >= df['nhhigh']),
            (df['nvalue'] >= df['nhigh']) & (df['nvalue'] < df['nhhigh']),
            (df['nvalue'] <= df['nllow']),
            (df['nvalue'] <= df['nlow']) & (df['nvalue'] > df['nllow'])
        ]
        df['threshold'] = np.select(conditions, [2, 1, -2, -1], default=0)

        df['rangeMax'] = df['vparam'].map(lambda x: preset_params.get(x, {}).get('nRangeMax', None))
        df['rangeMin'] = df['vparam'].map(lambda x: preset_params.get(x, {}).get('nRangeMin', None))

        df['rangeMax'] = pd.to_numeric(df['rangeMax'], errors='coerce')
        df['rangeMin'] = pd.to_numeric(df['rangeMin'], errors='coerce')
        
        cond_min = df['rangeMin'].isna() | (df['nvalue'] >= df['rangeMin'])
        cond_max = df['rangeMax'].isna() | (df['nvalue'] <= df['rangeMax'])
        
        df = df[cond_min & cond_max].copy()

        # Concat Vparam
        df['vparam'] = df['vwctid'].astype(str) + '_' + df['vmachineid'].astype(str) + '_' + df['vparam'].astype(str)

        group_cols = [
            'time_dt', 'time', 'dcrea', 'dmodi', 'vwctid', 'vmachineid', 
            'vparam', 'nhhigh', 'nhigh', 'nllow', 'nlow', 'catg', 'threshold',
            'rangeMax', 'rangeMin'
        ]
        avail_group_cols = [c for c in group_cols if c in df.columns]
        df = df.groupby(avail_group_cols, as_index=False).agg({'nvalue': 'mean'})

        final_cols = [
            'time', 'dcrea', 'dmodi', 'vwctid', 'vmachineid', 'vparam', 
            'nvalue', 'nhhigh', 'nhigh', 'nllow', 'nlow', 'catg', 'threshold', 
            'rangeMax', 'rangeMin'
        ]
        df = df[[c for c in final_cols if c in df.columns]]

    except Exception as e:
        logger.error(f"Kesalahan rules cleansing: {e}")
        return False

    sukses_pq = export_to_parquet(df, base_output_path)
    sukses_js = export_to_json(df, base_output_path)
    
    return sukses_pq and sukses_js