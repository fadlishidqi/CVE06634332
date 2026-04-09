from apps.common.filterTime import filterTime

def main():
    pathSource = "apps/data/result/aggregatedTime.json" 
    pathTarget = "apps/data/result/filteredTime.json"
    
    keyStart = "t"
    keyEnd = "dcrea"
    maxDelta = 1
    unitDelta = "hour"

    print("Memulai proses filter time delta...")
    hasil = filterTime(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keyStart=keyStart,
        keyEnd=keyEnd,
        maxDelta=maxDelta,
        unitDelta=unitDelta
    )

    if hasil is True:
        print(f"Selesai! File hasil filter waktu berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:100]}...\n")
    else:
        print("Gagal menjalankan filter waktu JSON.\n")

if __name__ == "__main__":
    main()