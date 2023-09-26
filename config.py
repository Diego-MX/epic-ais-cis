# pylint: disable=missing-module-docstring
import os
import pydantic     # pylint: disable=unused-import
# PYDANTIC no se usa aquí directamente, pero sí a través de EPIC_PY.
# Importarlo antes puede evadir un ImportError a la hora de cargarlo desde EPIC_PY.

from epic_py.delta import TypeHandler
from epic_py.platform import EpicIdentity


## Infrastructure resources are defined here.
SETUP_KEYS = {
    'qas': {
        'service-principal': { 
            'client_id'      : 'sp-core-events-client',         #'QAS_SP_CLIENT',
            'client_secret'  : 'sp-core-events-secret',         #'QAS_SP_SECRET',
            'subscription_id': 'sp-core-events-subscription',   #'QAS_SP_SUBSTN',
            'tenant_id'      : 'aad-tenant-id'},                #'AAD_TENANT'},
        'databricks-scope': 'eh-core-banking'}, 
    'stg': {
        'service-principal': { #                                # oauth-databricks-qas
            'client_id'      : 'sp-core-events-client',         #'QAS_SP_CLIENT',
            'client_secret'  : 'sp-core-events-secret',         #'QAS_SP_SECRET', 
            'subscription_id': 'sp-core-events-suscription',    #'QAS_SP_SUBSTN', 
            'tenant_id'      : 'aad-tenant-id'},                #'AAD_TENANT'},
        'databricks-scope': 'eh-core-banking'}, 
    'prd': {
        'service-principal': {
            'client_id'      : 'sp-collections-client', # 
            'client_secret'  : 'sp-collections-secret', #          
            'subscription_id': 'sp-collections-subscription', # 
            'tenant_id'      : 'aad-tenant-id'}, 
        'databricks-scope': 'cx-collections'},  #   eh-core-banking
}

AZURE_RESOURCES = {
    'qas': {
        'keyvault' : 'kv-cx-data-qas',
        'storage'  : 'stlakehyliaqas', 
        'blob_path': "ops/fraud-prevention"}, 
    'stg': {
        'keyvault' : 'kv-cx-data-stg',
        'storage'  : 'stlakehyliastg', 
        'blob_path': "ops/fraud-prevention"}, 
    'prd': {
        'keyvault' : 'kv-cx-data-prd',
        'storage'  : 'stlakehyliaprd',
        'blob_path': "ops/fraud-prevention"}
}

DBKS_MAPPING = { # Key from Excel Refs, Value on DBKS metastore.
    'gld_client_file'         : 'din_clients.gld_client_file',
    'gld_cx_collections_loans': 'nayru_accounts.gld_cx_collections_loans'
}

## Project-wide technical variables.

ENV = os.getenv('ENV_TYPE')
SERVER = os.getenv('SERVER_TYPE')

app_agent = EpicIdentity.create(server=SERVER, config=SETUP_KEYS[ENV])
app_resourcer = app_agent.get_resourcer(AZURE_RESOURCES[ENV], check_all=False)

dbks_tables = DBKS_MAPPING

blob_path = (app_resourcer.get_resource_url('abfss', 'storage',
        container='gold', blob_path=True))

falcon_handler = TypeHandler({
    'int' : {}, 'long': {},
    'dbl' : {}, 'str' : {},
    'date': {'NA_str': ' '*8},
    'ts'  : {'NA_str': ' '*6}})

falcon_rename = {
    'FieldName' : 'name',
    'tabla'     : 'table',
    'columna'   : 'column',
    'pytype'    : 'pytype',
    'Size'      : 'len'}
