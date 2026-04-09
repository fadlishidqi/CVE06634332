from apps.common.filterRange import filterRange

def main():
    pathSource = "apps/data/result/comparedValue.json" 
    pathTarget = "apps/data/result/filteredRange.json"
    pathPreset = "apps/preset/preset.json"

    print("Memulai proses Filter Range...")
    
    hasil = filterRange(
        pathSource=pathSource,
        pathTarget=pathTarget,
        pathPreset=pathPreset,
        keyValue="nvalue",
        keyWct="wct",
        keyTech="technum",
        keyParam="param"
    )

    if hasil is True:
        print(f"Selesai! File hasil filter range berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya preview: {hasil[:150]}...\n")
    else:
        print("Gagal menjalankan filter range JSON.\n")

if __name__ == "__main__":
    main()