from apps.common.normalization import normalization

def main():
    pathSource = "apps/data/result/outlierHandled.json" 
    pathTarget = "apps/data/result/normalizedData.json"
    pathPreset = "apps/preset/preset.json"

    print("Memulai proses Normalisasi Data Absolute...")
    
    hasil = normalization(
        pathSource=pathSource,
        pathTarget=pathTarget,
        pathPreset=pathPreset,
        keyValue="nvalue",
        keyWct="wct",
        keyTech="technum",
        keyParam="param"
    )

    if hasil is True:
        print(f"Selesai! File data yang dinormalisasi (0-1) berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
    else:
        print("Gagal menjalankan fungsi Normalisasi.\n")

if __name__ == "__main__":
    main()