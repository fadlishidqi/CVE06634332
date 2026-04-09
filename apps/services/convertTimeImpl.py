from apps.common.convertTime import convertTime

def main():
    pathSource = "apps/data/result/renamedData.json" 
    pathTarget = "apps/data/result/convertedTime.json"
    keySource = ["t", "dcrea"] 
    formatTime = "epoch_ms" 

    print(f"Memulai proses convert format waktu JSON menjadi {formatTime}...")
    hasil = convertTime(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keySource=keySource,
        formatTime=formatTime
    )

    if hasil is True:
        print(f"Selesai! File hasil convert waktu berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:100]}...\n")
    else:
        print("Gagal menjalankan convert waktu JSON.\n")

if __name__ == "__main__":
    main()