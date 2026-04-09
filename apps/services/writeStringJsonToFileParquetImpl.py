from apps.common.writeStringJsonToFileParquet import writeStringJsonToFileParquet
import json

def main():
    # Simulasi string JSON yang ada di memori
    dummy_data = [{"t": "05-01-2026", "value": 100}, {"t": "05-01-2026", "value": 110}]
    stringJson = json.dumps(dummy_data)
    
    pathTarget = "apps/data/result/writtenStringJson.parquet"

    print("Memulai proses penulisan String JSON ke file Parquet...")
    hasil = writeStringJsonToFileParquet(
        stringJson=stringJson, 
        pathTarget=pathTarget
    )

    if hasil is True:
        print(f"Selesai! String JSON berhasil disimpan menjadi file Parquet di: {pathTarget}")
    else:
        print("Gagal write string ke file Parquet.")

if __name__ == "__main__":
    main()