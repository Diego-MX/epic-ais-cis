from pathlib import Path
import platform
import re

sistema = platform.system()
print(f"Estamos en {sistema}")

A_FILE = "refs/Security Info.xlsx.lnk"

if isinstance(A_FILE, str):
    A_FILE = Path(A_FILE)
    print(A_FILE)
    print(type(A_FILE))

if isinstance(A_FILE, Path):
    file_ext = re.findall(r"\.([A-Za-z]{3,4})\.lnk", A_FILE.name)[0]
else:
    raise FileNotFoundError("Couldn't determine file extension.") 
