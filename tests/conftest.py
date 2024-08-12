import os
import sys 

import dependencies as deps

def pytest_configure():
    # Instalación de paquetes sólo para Databricks. 
    if 'DATABRICKS_RUNTIME_ENVIRONMENT' in os.environ: 
        sys.dont_write_bytecode = True
        deps.install_reqs()
    
    # Los imports los paso a los archivos donde se usan. 
    # from epic_py.platform import AzureResourcer
    # from src import app_agent, app_resourcer, dbks_tables, app_path, app_abfss
