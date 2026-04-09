from apps.common.loadFileParquetToString import loadFileParquetToString

def main():
    pathSource = "apps/data/raw/POLIMER_NH2-TEMP_3M2026.parquet"

    print("Memulai proses load Parquet ke memory (sebagai string JSON)...")
    hasil_string = loadFileParquetToString(pathSource=pathSource)

    if isinstance(hasil_string, str):
        print("Selesai! File Parquet berhasil di-load dan dikonversi menjadi String JSON.")
        print(f"Bentuk datanya: {hasil_string[:200]}...\n")
    else:
        print("Gagal load file Parquet.")

if __name__ == "__main__":
    main()