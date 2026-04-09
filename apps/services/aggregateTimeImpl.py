from apps.common.aggregateTime import aggregateTime

def main():
    pathSource = "apps/data/result/convertedTime.json" 
    pathTarget = "apps/data/result/aggregatedTime.json"
    
    keyTime = "t"
    keyValues = ["nvalue", "llow", "low", "vvalue", "nhhigh", "nhigh", "nllow", "nlow", "catg"]
    
    # Metode agregasi (berurutan sesuai dengan keyValues di atas)
    # Pilihan: "mean", "min", "max", "last"
    aggMethods = ["mean", "last", "last", "last", "last", "last", "last", "last", "last"]
    keyGroups = ["wct", "technum", "param"]

    print("Memulai proses agregasi waktu JSON...")

    hasil = aggregateTime(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keyTime=keyTime,
        keyValues=keyValues,
        aggMethods=aggMethods,
        keyGroups=keyGroups
    )

    if hasil is True:
        print(f"Selesai! File hasil agregasi berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:150]}...\n")
    else:
        print("Gagal menjalankan agregasi waktu JSON.\n")

if __name__ == "__main__":
    main()