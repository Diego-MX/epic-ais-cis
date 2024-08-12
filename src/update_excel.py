"""Este todavía no está listo."""

from pathlib import Path
import platform 
import re 

import dependencies as deps

deps.install_reqs()

sistema = platform.system()
print(f"Estamos en {sistema}")


a_file = "refs/Security Info.xlsx.lnk"

if isinstance(a_file, str):
    a_file = Path(a_file)
    print(a_file)
    print(type(a_file))

if isinstance(a_file, Path):
    file_ext = re.findall(r"\.([A-Za-z]{3,4})\.lnk", a_file.name)[0]
else:
    raise ValueError("Couldn't determine file extension.")
    

# spark = SparkSession.builder.getOrCreate()
# dbutils = DBUtils(spark)

# ref_path = "refs/Security Info.xlsx.lnk"
# df = pd.read_feather("../refs/upload-specs/accounts_cols.feather")
# Link = "https://bineomex.sharepoint.com/:x:/r/sites/EngineeringX/Documentos%20compartidos/PC/Security%20Info.xlsx?d=webfd5bd795cf4def965a997d955971fb&csf=1&web=1&e=ZFDhJi"

# Respuesta = requests.get(Link)
# print(Respuesta)
# if Respuesta == 200:
#     print("exito al conectar")

# else:
#     print("Fallo")

# print(df)