from apps.common.pivotTable import pivotTable

def main():
    pathSource = "apps/data/result/filteredRange.json" 
    
    pathPreset = "apps/preset/preset.json"
    pathTarget = "apps/data/result/pivotedData.json"
    
    indexCols = ["t", "wct", "technum"]
    pivotCol = "param"
    timeCol = "t"

    print("Memulai proses Pivot Data...")
    hasil = pivotTable(
        pathSource=pathSource,
        pathTarget=pathTarget,
        pathPreset=pathPreset,
        indexCols=indexCols,
        pivotCol=pivotCol,
        timeCol=timeCol
    )

    if hasil is True:
        print(f"Selesai! Data berhasil di-pivot sesuai preset di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Preview: {hasil[:500]}...\n")
    else:
        print("Gagal menjalankan pivot table.\n")

if __name__ == "__main__":
    main()