"""Este código tiene como finalidad la limpieza de los demás códigos,
para ello se utiliza pylint"""

import subprocess

# Define la ruta a tu archivo Python en DBFS
# FILE_PATH_DATABRICKS = "/Workspace/Repos/juan.v@bineo.com/fraud-prevention/notebooks/1 🦝 falcon reports.py"
FILE_PATH_LOCAL = "/Users/juanrodrigo/Documents/Repositorios_GitHub/Repositorio_Fraude/notebooks/dependence/1_dim_client_x.py"

# Ejecuta pylint
result = subprocess.run(['pylint', FILE_PATH_LOCAL], capture_output=True, text=True)

# Muestra la salida
print(result.stdout)
print()
print(result.stderr)

# Finite Incantatem
