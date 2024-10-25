"""Configuración de las pruebas unitarias"""
from sys import dont_write_bytecode
import warnings

import dbks_dependencies as deps
import config as cfg


def pytest_configure():
    """Lo que se se coloque aquí afectara en la ejecución de las pruebas"""
    dont_write_bytecode = True
    deps.from_reqsfile(cfg.REQS_FILE)
    warnings.filterwarnings("ignore", category=DeprecationWarning)

# Finite Incatatem
