import pandas as pd
import json
from pathlib import Path
import logging

from apps.common.parquet_exporter import export_to_parquet
from apps.common.json_exporter import export_to_json
from apps.common.outlier import apply_iqr_outlier
from apps.common.normalization import apply_minmax_normalization

logger = logging.getLogger(__name__)

def process_learning_data(base_input_path: Path | str, base_output_path: Path | str) -> bool:
    input_path = Path(str(base_input_path) + ".parquet")
    preset_path = Path(__file__).parent.parent / 'preset' / 'preset.json'

    if not input_path.exists():
        logger.error(f"File input tidak ditemukan: {input_path}")
        return False

    # Read preset.json
    try:
        with open(preset_path, 'r') as f:
            preset_data = json.load(f)
        
        global_settings = preset_data.get('global_settings', {})
        preset_params = preset_data.get('parameters', {})
        
        is_normalization = global_settings.get('normalization', False)
        is_outlier = global_settings.get('outlier', False)
    except Exception as e:
        logger.error(f"Gagal membaca preset.json: {e}")
        return False

    try:
        df = pd.read_parquet(input_path)
        
        df['time'] = pd.to_datetime(df['time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
        df = df.dropna(subset=['time', 'vparam', 'nvalue'])

        # OUTLIER
        if is_outlier:
            logger.info("Mengaktifkan Filter Outlier (IQR)...")
            df = apply_iqr_outlier(df)

        # NORMALIZATION
        if is_normalization:
            logger.info("Mengaktifkan Normalisasi Min-Max...")
            df = apply_minmax_normalization(df)

        # PIVOT DATA
        df_pivot = df.pivot_table(
            index='time',
            columns='vparam',
            values=['nvalue', 'catg', 'threshold'],
            aggfunc='first'
        )

        new_columns = []
        valid_nvalue_cols = []
        valid_crit_cols = []
        valid_tresh_cols = []

        for val, param in df_pivot.columns:
            prefix = 'critical' if val == 'catg' else 'treshold' if val == 'threshold' else val
            col_name = f"{prefix}_{param}"
            new_columns.append(col_name)

            base_param = None
            for key in preset_params.keys():
                if key in param:
                    base_param = key
                    break
            
            if val == 'nvalue':
                valid_nvalue_cols.append(col_name)
            elif val == 'catg':
                if base_param and preset_params[base_param].get('kategori', False):
                    valid_crit_cols.append(col_name)
            elif val == 'threshold':
                if base_param and preset_params[base_param].get('treshold', False):
                    valid_tresh_cols.append(col_name)

        df_pivot.columns = new_columns
        df_pivot = df_pivot.sort_index().ffill().bfill().reset_index()
        df_pivot['time'] = df_pivot['time'].dt.strftime('%d-%m-%Y %H:%M:%S')

        final_cols_ordered = ['time'] + valid_nvalue_cols + valid_crit_cols + valid_tresh_cols
        
        df_final = df_pivot[final_cols_ordered].copy()


    except Exception as e:
        logger.error(f"Kesalahan data learning: {e}")
        return False

    sukses_pq = export_to_parquet(df_final, base_output_path)
    sukses_js = export_to_json(df_final, base_output_path)
    
    return sukses_pq and sukses_js