from apps.common.forwardFill import forwardFill

def main():
    pathSource = "apps/data/result/pivotedData.json"
    pathTarget = "apps/data/result/final_output.json"
    pathPreset = "apps/preset/preset.json"
    
    sortCols = ["t"]
    groupCols = ["wct", "technum"]
    dropNulls = True 
    
    print("Memulai proses Forward Fill sekaligus Validator...")
    hasil = forwardFill(
        pathSource=pathSource,
        pathTarget=pathTarget,
        pathPreset=pathPreset,
        sortCols=sortCols,
        groupCols=groupCols,
        dropNulls=dropNulls
    )

    if hasil is True:
        print(f"Selesai! File bersih tanpa Null berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
    else:
        print("Gagal menjalankan Forward Fill dan Validasi.\n")

if __name__ == "__main__":
    main()