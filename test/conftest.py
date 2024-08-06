import sys 
import dependencies as deps


def pytest_configure():
    sys.dont_write_bytecode = True
    deps.install_reqs()
    
    from epic_py.platform import AzureResourcer
    from src import app_agent, app_resourcer, dbks_tables, app_path, app_abfss
