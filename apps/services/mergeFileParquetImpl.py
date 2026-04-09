from apps.common.mergeFileParquet import mergeFileParquet

def main():
    pathSource = [
        "apps/data/raw/POLIMER_NH1-TEMP_3M2026.parquet",
        "apps/data/raw/POLIMER_NH2-TEMP_3M2026.parquet"
    ]
    
    pathTarget = "apps/data/result/mergedData.parquet" 

    print("Memulai proses merge file Parquet...")
    hasil = mergeFileParquet(
        pathSource=pathSource,
        pathTarget=pathTarget
    )

    if hasil is True:
        print(f"Selesai! File Parquet gabungan berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data Parquet digabung dan disimpan di memori sebagai String JSON.")
        print(f"Bentuk datanya: {hasil[:200]}...\n")
    else:
        print("Gagal menjalankan merge file Parquet.\n")

if __name__ == "__main__":
    main()