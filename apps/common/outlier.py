import pandas as pd
import logging

logger = logging.getLogger(__name__)

def apply_iqr_outlier(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["vparam", "nvalue", "rangeMax", "rangeMin"]
    if not all(col in df.columns for col in required_cols):
        logger.warning(f"Kolom tidak lengkap untuk filter IQR Outlier. Diperlukan: {required_cols}")
        return df

    try:
        for col in ["nvalue", "rangeMax", "rangeMin"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # HITUNG IQR (PER vparam)
        group_col = ["vparam"]
        Q1 = df.groupby(group_col)["nvalue"].transform(lambda x: x.quantile(0.25))
        Q3 = df.groupby(group_col)["nvalue"].transform(lambda x: x.quantile(0.75))

        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        # FILTER IQR + RANGE FINAL SAFETY
        cond_min = df["rangeMin"].isna() | (df["nvalue"] > df["rangeMin"])
        cond_max = df["rangeMax"].isna() | (df["nvalue"] < df["rangeMax"])
        cond_iqr = (df["nvalue"] >= lower) & (df["nvalue"] <= upper)

        # Filter data
        before_iqr = len(df)
        df_clean = df[cond_min & cond_max & cond_iqr].copy()
        after_iqr = len(df_clean)
        
        logger.info(f"IQR Outlier Filtered: {before_iqr} -> {after_iqr} baris")
        return df_clean

    except Exception as e:
        logger.error(f"Error saat menjalankan fungsi IQR Outlier: {e}")
        return df