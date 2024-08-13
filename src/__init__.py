# SRC asume que EPICPY está instalado.

from epic_py.platform import EpicIdentity
import config as cfg


app_agent = EpicIdentity.create(server=cfg.SERVER, config=cfg.SETUP_KEYS[cfg.ENV]) 
app_resourcer = app_agent.get_resourcer(cfg.AZURE_RESOURCES[cfg.ENV], check_all=False)
app_path = app_resourcer['storage-paths']['fraud']
app_abfss = (app_resourcer.get_resource_url('abfss', 'storage',
        container='gold', blob_path=app_path))

dbks_tables = {kk: f"{cfg.ENV}.{tt}"
    for kk, tt in cfg.DBKS_MAPPING.items()}

falcon_types = {
    'int' : {'NA_str': ''}, 
    'long': {'NA_str': ''},
    'dbl' : {'NA_str': ''}, 
    'str' : {'NA_str': '', 'encoding': 'ascii'},
    'date': {'NA_str': ' '*8, 'c_format': '%8s'},
    'ts'  : {'NA_str': ' '*6, 'c_format': '%6s'}}

falcon_rename = {
    'FieldName' : 'name',
    'tabla'     : 'table',
    'columna'   : 'column',
    'pytype'    : 'pytype',
    'Size'      : 'len'}
