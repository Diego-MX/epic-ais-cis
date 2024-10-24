"""Este código tiene como finalidad la limpieza de los demás códigos,
para ello se utiliza pylint"""

import subprocess

# Define la ruta a tu archivo Python en DBFS
FILE_PATH = "/Workspace/Repos/juan.v@bineo.com/fraud-prevention/test/function_test.py"

# Ejecuta pylint
result = subprocess.run(['pylint', FILE_PATH], capture_output=True, text=True)

# Muestra la salida
print(result.stdout)
print()
print(result.stderr)

# Finite Incatatem
