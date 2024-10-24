import subprocess

# Define la ruta a tu archivo Python en DBFS
file_path = "/Workspace/Repos/juan.v@bineo.com/fraud-prevention/test/function_test.py"

# Ejecuta pylint
result = subprocess.run(['pylint', file_path], capture_output=True, text=True)

# Muestra la salida
print(result.stdout)
print()
print(result.stderr)

# Finite Incatatem 
