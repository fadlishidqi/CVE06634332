from apps.common.loadFileJsonToString import loadFileJsonToString
from apps.common.writeStringJsonToFileJson import writeStringJsonToFileJson

def main():
    pathSource = "apps/data/result/final_output.json"
    print(f"1. Membaca data dari {pathSource} ke memori...")
    
    real_string_json = loadFileJsonToString(pathSource=pathSource)
    
    if not isinstance(real_string_json, str):
        print("Gagal mendapatkan string JSON asli. Proses dihentikan.")
        return

    print("Berhasil! Data sekarang berada di memori sebagai String.\n")

    pathTarget = "apps/data/result/writtenStringJson.json"
    print(f"2. Memulai proses penulisan String JSON asli ke file fisik ({pathTarget})...")
    
    hasil = writeStringJsonToFileJson(
        stringJson=real_string_json, 
        pathTarget=pathTarget
    )

    if hasil is True:
        print(f"Selesai! String memori berhasil disimpan kembali menjadi file JSON.")
    else:
        print("Gagal write string ke file JSON.")

if __name__ == "__main__":
    main()