import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def export_to_json(df: pd.DataFrame, base_output_path: Path | str) -> bool:
    try:
        base_path = Path(base_output_path)
        base_path.parent.mkdir(parents=True, exist_ok=True)
        
        json_path = base_path.with_suffix('.json')
        
        df.to_json(json_path, orient='records', date_format='iso', indent=4)
        
        logger.info(f"Berhasil export ke JSON: {json_path.name} ({len(df)} baris)")
        return True
    except Exception as e:
        logger.error(f"Gagal export ke JSON: {e}")
        return False