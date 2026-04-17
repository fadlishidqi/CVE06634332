from apps.common.convertJsonToParquet import convertJsonToParquet

def main():
    pathSource = "apps/data/result/ISD OPR_AHMMO RAW.json"
    pathTarget = "apps/data/result/ISD OPR_AHMMO RAW.parquet"

    print(f"Memulai proses konversi dari JSON ke Parquet...")
    print(f"Source : {pathSource}")
    print(f"Target : {pathTarget}\n")
    
    hasil = convertJsonToParquet(
        pathSource=pathSource, 
        pathTarget=pathTarget
    )

    if hasil is True:
        print(f"Selesai! File JSON berhasil dikonversi dan disimpan menjadi file Parquet.")
    else:
        print("Gagal melakukan konversi file JSON ke Parquet.")

if __name__ == "__main__":
    main()