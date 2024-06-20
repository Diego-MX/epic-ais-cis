## Project-wide technical variables.
import os 
import dependencies as deps 
deps.gh_epicpy('meetme-1', 
    tokenfile='../user_databricks.yml', typing=False, verbose=True)
from epic_py.platform import EpicIdentity 
from epic_py.delta import TypeHandler
import config as cfg

app_agent = EpicIdentity.create(server=cfg.SERVER, config=cfg.SETUP_KEYS[cfg.ENV])
app_resourcer = app_agent.get_resourcer(cfg.AZURE_RESOURCES[cfg.ENV], check_all=False)

dbks_tables = cfg.DBKS_MAPPING
blob_path = (app_resourcer.get_resource_url('abfss', 'storage',
        container='gold', blob_path=True))

falcon_handler = TypeHandler({
    'int' : {'NA_str': ''}, 
    'long': {'NA_str': ''},
    'dbl' : {'NA_str': ''}, 
    'str' : {'NA_str': '', 'encoding': 'ascii'},
    'date': {'NA_str': ' '*8, 'c_format': '%8s'},
    'ts'  : {'NA_str': ' '*6, 'c_format': '%6s'}})

falcon_rename = {
    'FieldName' : 'name',
    'tabla'     : 'table',
    'columna'   : 'column',
    'pytype'    : 'pytype',
    'Size'      : 'len'}
