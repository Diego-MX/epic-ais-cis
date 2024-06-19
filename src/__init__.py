# SRC asume que EPICPY está instalado.

import config as c
from epic_py.platform import EpicIdentity
from epic_py.delta import TypeHandler

app_agent = EpicIdentity.create(server=c.SERVER, config=c.SETUP_KEYS[c.ENV])
app_resourcer = app_agent.get_resourcer(c.AZURE_RESOURCES[c.ENV], check_all=False)

dbks_tables = c.DBKS_MAPPING
blob_path = app_resourcer['storage-paths']['fraud']
blob_abfss = (app_resourcer.get_resource_url('abfss', 'storage',
        container='gold', blob_path=pre_path))

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
