from apps.common.loadFileJsonToString import loadFileJsonToString

def main():
    pathSource = "apps/data/result/presetViewsData.json"

    print("Memulai proses load JSON ke memory (string)...")
    hasil_string = loadFileJsonToString(pathSource=pathSource)

    if isinstance(hasil_string, str):
        print("Selesai! File berhasil di-load menjadi String JSON.")
        print(f"Bentuk datanya: {hasil_string[:200]}...\n")
    else:
        print("Gagal load file JSON.")

if __name__ == "__main__":
    main()