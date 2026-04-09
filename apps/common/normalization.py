import pandas as pd
import logging

logger = logging.getLogger(__name__)

def apply_minmax_normalization(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["nvalue", "rangeMax", "rangeMin"]
    if not all(col in df.columns for col in required_cols):
        logger.warning(f"Kolom tidak lengkap untuk Normalisasi. Diperlukan: {required_cols}")
        return df
        
    try:
        for col in required_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
        range_diff = df['rangeMax'] - df['rangeMin']
        
        # Masking
        mask = (range_diff > 0) & df['rangeMin'].notnull() & df['rangeMax'].notnull()
        
        df.loc[mask, 'nvalue'] = (df.loc[mask, 'nvalue'] - df.loc[mask, 'rangeMin']) / range_diff[mask]
        
        logger.info("Min-Max Normalization berhasil diterapkan.")
        return df
        
    except Exception as e:
        logger.error(f"Error saat menjalankan fungsi Normalisasi: {e}")
        return df