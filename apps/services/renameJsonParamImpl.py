from apps.common.renameJsonParam import renameJsonParam

def main():
    pathSource = "apps/data/result/rawMerge.json" 
    
    pathTarget = "apps/data/result/renamedData.json"
    
    keySource = ["time", "vwctid", "vmachineid", "vparam", "catg",  "nllow", "nlow", "nhigh", "nhhigh"]
    keyTarget = ["t", "wct", "technum", "param", "category", "llow", "low", "high", "hhigh"]

    print("Memulai proses rename parameter JSON...")
    hasil = renameJsonParam(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keySource=keySource,
        keyTarget=keyTarget
    )

    if hasil is True:
        print(f"Selesai! File hasil rename berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:100]}...\n")
    else:
        print("Gagal menjalankan rename parameter JSON.\n")

if __name__ == "__main__":
    main()