from apps.common.decryptData import decryptData

key = "85FQ1ZWxml8mODnfXhO13LT4cCIsHBOLewFZdw39DFs="
encrypted = "gAAAAABp4Ym0D2I_zs8zUdc608lSy9c1GkIR3X8aslFdPAfJF0vvpz9sMs5NlvL8mP2OlfTK5Nt-hE8Zra-BZ_JADytleS_nSA=="

result = decryptData(key=key, encryptText=encrypted)
print(result)