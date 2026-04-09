import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def export_to_parquet(df: pd.DataFrame, base_output_path: Path | str) -> bool:
    try:
        base_path = Path(base_output_path)
        base_path.parent.mkdir(parents=True, exist_ok=True)
        
        parquet_path = base_path.with_suffix('.parquet')
        df.to_parquet(parquet_path, engine='pyarrow', index=False)
        
        logger.info(f"Berhasil export ke Parquet: {parquet_path.name} ({len(df)} baris)")
        return True
    except Exception as e:
        logger.error(f"Gagal export ke Parquet: {e}")
        return False