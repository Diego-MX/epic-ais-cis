# SRC asume que EPICPY está instalado.
import os
from dotenv import load_dotenv
from epic_py.platform import EpicIdentity
import config as cfg

load_dotenv(override=True)

FILE_REF = "refs/Security Info.xlsx.lnk"

ENV = os.getenv('ENV_TYPE')
SERVER = os.getenv('SERVER_TYPE')


app_agent = EpicIdentity.create(server=SERVER, config=cfg.SETUP_KEYS[ENV])
app_resourcer = app_agent.get_resourcer(cfg.AZURE_RESOURCES[ENV], check_all=False)

app_path = app_resourcer['storage-paths']['fraud']
app_abfss = (app_resourcer.get_resource_url('abfss', 'storage',
        container='gold', blob_path=app_path))

dbks_tables = {kk: f"{ENV}.{tt}"
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
