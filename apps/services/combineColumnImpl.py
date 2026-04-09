from apps.common.combineColumn import combineColumn

def main():
    pathSource = "apps/data/result/filteredRange.json"
    pathTarget = "apps/data/result/combinedParam.json"

    colsToCombine = ['wct', 'technum', 'param']
    targetCol = 'param'
    separator = '_'

    print("Memulai proses penggabungan kolom...")

    sukses = combineColumn(
        pathSource=pathSource,
        pathTarget=pathTarget,
        colsToCombine=colsToCombine,
        targetCol=targetCol,
        separator=separator
    )

    if sukses is True:
        print(f"Selesai! Kolom berhasil digabungkan menjadi kolom '{targetCol}' dan disimpan ke file.")
    elif isinstance(sukses, str):
        print(f"Selesai! Kolom berhasil digabungkan menjadi kolom '{targetCol}'. Data masuk ke memori.")
        print(f"Bentuk datanya: {sukses[:200]}...\n")
    else:
        print("Proses gagal. Silakan cek log.")

if __name__ == "__main__":
    main()