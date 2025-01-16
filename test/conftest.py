"""Configuración de las pruebas unitarias"""
import warnings

import dbks_dependencies as deps # pylint: disable = import-error
import config as cfg # pylint: disable = import-error

def pytest_configure():
    """Lo que se se coloque aquí afectara en la ejecución de las pruebas"""
    deps.from_reqsfile(cfg.REQS_FILE)
    warnings.filterwarnings("ignore", category=DeprecationWarning)

# Finite Incantatem
