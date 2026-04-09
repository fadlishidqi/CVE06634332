from apps.common.compareValue import compareValue

def main():
    pathSource = "apps/data/result/replacedData.json"
    pathTarget = "apps/data/result/comparedValue.json"

    keySource = "nvalue"
    keyResult = "threshold"
    defaultValue = 0

    keyCompare = ["llow", "hhigh", "low", "high"] 
    operators = ["<", ">", "<", ">"]
    resultCompare = [-2, 2, -1, 1]

    print("Memulai proses komparasi data JSON...")
    hasil = compareValue(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keySource=keySource,
        keyCompare=keyCompare,
        operators=operators,
        resultCompare=resultCompare,
        defaultValue=defaultValue,
        keyResult=keyResult
    )

    if hasil is True:
        print(f"Selesai! File hasil komparasi berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:100]}...\n")
    else:
        print("Gagal menjalankan komparasi data JSON.\n")

if __name__ == "__main__":
    main()