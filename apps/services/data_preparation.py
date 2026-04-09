import pandas as pd
from pathlib import Path
import logging

from apps.common.parquet_exporter import export_to_parquet
from apps.common.json_exporter import export_to_json

logger = logging.getLogger(__name__)

def process_and_merge_json(raw_dir_path: Path | str, base_output_path: Path | str) -> bool:
    raw_dir = Path(raw_dir_path)

    target_columns = [
        "time", "dcrea", "dmodi", "vwctid", "vmachineid", 
        "vparam", "nvalue", "nhhigh", "nhigh", "nllow", "nlow", "catg"
    ]

    all_dataframes = []
    json_files = list(raw_dir.glob("*.json"))

    if not json_files:
        logger.warning(f"Tidak ada file JSON di {raw_dir}")
        return False

    logger.info(f"Memproses {len(json_files)} file JSON untuk Preparation...")

    for file_path in json_files:
        try:
            df = pd.read_json(file_path)
            available_cols = [col for col in target_columns if col in df.columns]
            all_dataframes.append(df[available_cols])
        except Exception as e:
            logger.error(f"Error memproses {file_path.name}: {e}")

    if not all_dataframes:
        return False

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    combined_df = combined_df.drop_duplicates()
    
    sukses_pq = export_to_parquet(combined_df, base_output_path)
    sukses_js = export_to_json(combined_df, base_output_path)
    
    return sukses_pq and sukses_js