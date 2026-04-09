from apps.common.replaceData import replaceData

def main():
    pathSource = "apps/data/result/filteredTime.json" 
    
    pathTarget = "apps/data/result/replacedData.json"
    
    keySource = ["category"]
    fromData = ["C", "NC"]
    toData = [1, 0]

    print("Memulai proses replace data JSON...")
    hasil = replaceData(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keySource=keySource,
        fromData=fromData,
        toData=toData
    )

    if hasil is True:
        print(f"Selesai! File hasil replace berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:100]}...\n")
    else:
        print("Gagal menjalankan replace data JSON.\n")

if __name__ == "__main__":
    main()