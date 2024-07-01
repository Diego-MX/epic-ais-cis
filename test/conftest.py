"""Script de configuración del test"""
from sys import dont_write_bytecode
import dbks_dependencies as deps
import config as cfg

def pytest_configure():
    """Función de configuración del test"""
    dont_write_bytecode = True
    deps.from_reqsfile(cfg.REQS_FILE)
