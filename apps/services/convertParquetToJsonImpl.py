from apps.common.convertParquetToJson import convertParquetToJson

def main():
    pathSource = "apps/data/raw/POLIMER_CUSHION-POSITION_3M2026.parquet"
    pathTarget = "apps/data/result/convertedFromParquet.json"

    print(f"Memulai proses konversi dari Parquet ke JSON...")
    print(f"Source : {pathSource}")
    print(f"Target : {pathTarget}\n")
    
    hasil = convertParquetToJson(
        pathSource=pathSource, 
        pathTarget=pathTarget
    )

    if hasil is True:
        print(f"Selesai! File Parquet berhasil dikonversi dan disimpan menjadi file JSON.")
    else:
        print("Gagal melakukan konversi file Parquet ke JSON.")

if __name__ == "__main__":
    main()