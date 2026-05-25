<<<<<<< HEAD
"""Este todavía no está listo."""

from pathlib import Path
import platform 
import re 

import dependencies as deps

deps.install_reqs()
=======
"""
Forma de subir los archivos requeridos para el notebook,
ya no se puede utilizar dado que seguridad dice que es 
peligroso
"""

import platform

import re
from pathlib import Path
>>>>>>> dev-juan

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
<<<<<<< HEAD
    raise ValueError("Couldn't determine file extension.")
    
=======
    raise Exception("Couldn't determine file extension.") # pylint: disable=broad-exception-raised
>>>>>>> dev-juan

# Finite Incantatem
