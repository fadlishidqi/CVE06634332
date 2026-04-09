import json
from apps.common.mergeJson import mergeJson
from apps.common.renameJsonParam import renameJsonParam
from apps.common.convertTime import convertTime
from apps.common.aggregateTime import aggregateTime
from apps.common.filterTime import filterTime
from apps.common.replaceData import replaceData
from apps.common.compareValue import compareValue
from apps.common.filterRange import filterRange

from apps.common.normalization import normalization
from apps.common.outlier import outlier

from apps.common.pivotTable import pivotTable
from apps.common.forwardFill import forwardFill
from apps.common.writeStringJsonToFileJson import writeStringJsonToFileJson

def main():
    print("Membaca preset.json...")
    preset_path = "apps/preset/preset.json"
    with open(preset_path, "r") as f:
        preset = json.load(f)
    
    do_normalization = False
    do_outlier = False
    for item in preset.get("global_settings", []):
        if item.get("key") == "normalization" and str(item.get("value")).lower() == "true":
            do_normalization = True
        if item.get("key") == "outlier" and str(item.get("value")).lower() == "true":
            do_outlier = True

    pathSource = [
        "apps/data/raw/CUSHION.json",
        "apps/data/raw/MAXPRESS.json",
        "apps/data/raw/NH1.json",
        "apps/data/raw/NH2.json",
    ]
    keySource_merge = ["time", "dcrea", "vwctid", "vmachineid", "vparam", "nvalue", "vvalue", "nhhigh", "nhigh", "nllow", "nlow", "catg"]
    
    keySource_rename = ["time", "vwctid", "vmachineid", "vparam", "catg",  "nllow", "nlow", "nhigh", "nhhigh"]
    keyTarget_rename = ["t", "wct", "technum", "param", "category", "llow", "low", "high", "hhigh"]

    pathTarget_final = "apps/data/result/final_output.json"


    def check_str(result, step_name: str) -> str:
        if not isinstance(result, str):
            print(f"Proses terhenti! Terjadi kesalahan pada tahap: {step_name}")
            exit(1)
        return result

    # EKSEKUSI
    print("\n[1/11] Memulai Merge JSON...")
    json_str = mergeJson(pathSource=pathSource, pathTarget=None, keySource=keySource_merge)
    json_str = check_str(json_str, "Merge JSON")

    print("[2/11] Rename Parameter...")
    json_str = renameJsonParam(pathSource=json_str, pathTarget=None, keySource=keySource_rename, keyTarget=keyTarget_rename)
    json_str = check_str(json_str, "Rename Parameter")

    print("[3/11] Convert Time...")
    json_str = convertTime(pathSource=json_str, pathTarget=None, keySource=["t", "dcrea"], formatTime="epoch_ms")
    json_str = check_str(json_str, "Convert Time")

    print("[4/11] Aggregate Time...")
    json_str = aggregateTime(
        pathSource=json_str, 
        pathTarget=None, 
        keyTime="t", 
        keyValues = ["nvalue", "llow", "low", "vvalue", "nhhigh", "nhigh", "nllow", "nlow", "catg"], 
        aggMethods = ["mean", "last", "last", "last", "last", "last", "last", "last", "last"],
        keyGroups=["wct", "technum", "param"]
    )
    json_str = check_str(json_str, "Aggregate Time")

    print("[5/11] Filter Time...")
    json_str = filterTime(
        pathSource=json_str, 
        pathTarget=None, 
        keyStart="t", 
        keyEnd="dcrea", 
        maxDelta=1, 
        unitDelta="hours"
    )
    json_str = check_str(json_str, "Filter Time")

    print("[6/11] Replace Data...")
    json_str = replaceData(
        pathSource=json_str, 
        pathTarget=None, 
        keySource=["category"],
        fromData=["C", "NC"],
        toData=[1, 0]
    )
    json_str = check_str(json_str, "Replace Data")

    print("[7/11] Compare Value (Mencari Threshold)...")
    json_str = compareValue(
        pathSource=json_str, 
        pathTarget=None, 
        keySource="nvalue", 
        keyCompare=["llow", "hhigh", "low", "high"],
        operators=["<", ">", "<", ">"],
        resultCompare=[-2, 2, -1, 1],
        defaultValue=0,
        keyResult="threshold"
    )
    json_str = check_str(json_str, "Compare Value")

    print("[8/11] Filter Range...")
    json_str = filterRange(
        pathSource=json_str, 
        pathTarget=None, 
        pathPreset=preset_path,
        keyValue="nvalue",
        keyWct="wct",
        keyTech="technum",
        keyParam="param"
    )
    json_str = check_str(json_str, "Filter Range")

    print("\nMengecek konfigurasi Normalization / Outlier dari preset.json...")
    if do_outlier:
        print("    -> Outlier bernilai TRUE, menjalankan proses Outlier...")
        json_str = outlier(
            pathSource=json_str, 
            pathTarget=None, 
            pathPreset=preset_path,
            groupCols=["wct", "technum", "param"],
            keyValue="nvalue"
        )
        json_str = check_str(json_str, "Outlier")
    else:
        print("    -> Outlier di-skip (FALSE).")

    if do_normalization:
        print("    -> Normalization bernilai TRUE, menjalankan proses Normalization...")
        json_str = normalization(
            pathSource=json_str, 
            pathTarget=None, 
            pathPreset=preset_path,
            keyValue="nvalue",
            keyWct="wct",
            keyTech="technum",
            keyParam="param"
        )
        json_str = check_str(json_str, "Normalization")
    else:
        print("    -> Normalization di-skip (FALSE).")

    print("\n[9/11] Pivot Table (Membentuk struktur tabel akhir)...")
    json_str = pivotTable(
        pathSource=json_str, 
        pathTarget=None,
        pathPreset=preset_path,
        indexCols=["t", "wct", "technum"],
        pivotCol="param",
        timeCol="t"
    )
    json_str = check_str(json_str, "Pivot Table")

    print("[10/11] Forward Fill (Mengisi nilai kosong & Hapus Null)...")
    json_str = forwardFill(
        pathSource=json_str,
        pathTarget=None,
        pathPreset=preset_path,
        sortCols=["t"],
        groupCols=["wct", "technum"],
        dropNulls=True
    )
    json_str = check_str(json_str, "Forward Fill")

    print(f"\n[11/11] Menyimpan hasil akhir ke file: {pathTarget_final}...")
    hasil_akhir = writeStringJsonToFileJson(stringJson=json_str, pathTarget=pathTarget_final)

    if hasil_akhir is True:
        print("\nData berhasil disimpan sesuai format yang diinginkan.")
    else:
        print("\nGagal menyimpan file hasil akhir.")

if __name__ == "__main__":
    main()