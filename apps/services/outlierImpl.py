from apps.common.outlier import outlier

def main():
    pathSource = "apps/data/result/filteredRange.json" 
    pathTarget = "apps/data/result/outlierHandled.json"
    pathPreset = "apps/preset/preset.json"

    groupCols = ["wct", "technum", "param"]
    keyValue = "nvalue"

    print(f"Memulai proses penanganan Outlier per grup: {groupCols}...")
    
    hasil = outlier(
        pathSource=pathSource,
        pathTarget=pathTarget,
        pathPreset=pathPreset,
        groupCols=groupCols,
        keyValue=keyValue
    )

    if hasil is True:
        print(f"Selesai! File bersih dari outlier berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
    else:
        print("Gagal menjalankan fungsi Outlier.\n")

if __name__ == "__main__":
    main()