import os

from epic_py.delta import EpicDataBuilder, TypeHandler
from epic_py.identity import EpicIdentity


## Infrastructure resources are defined here. 
SETUP_KEYS = {
    'qas': {
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
        'databricks-scope': 'eh-core-banking'},  # kv-zoras
}

AZURE_RESOURCES = {
    'qas': {
        'keyvault' : 'kv-cx-data-qas',
        'storage'  : 'stlakehyliaqas', 
        'blob_path': "ops/fraud-prevention"}, 
    'prd': {
        'keyvault' : 'kv-cx-data-prd',
        'storage'  : 'stlakehyliaprd', 
        'blob_path': "ops/fraud-prevention"}
}

DBKS_MAPPING = { # Key from Excel Refs, Value on DBKS metastore.  
    'gld_client_file'         : 'din_clients.gld_client_file', 
    'gld_cx_collections_loans': 'nayru_accounts.gld_cx_collections_loans'}



## Project-wide technical variables. 

ENV = os.getenv('ENV_TYPE')
SERVER = os.getenv('SERVER_TYPE')

app_agent = EpicIdentity.create(server=SERVER, config=SETUP_KEYS[ENV])
app_resourcer = app_agent.setup_resourcer(AZURE_RESOURCES[ENV])

dbks_tables = DBKS_MAPPING

falcon_handler = TypeHandler({
    'int' : {}, 'long': {}, 
    'dbl' : {}, 'str' : {}, 
    'date': {}, 'ts'  : {}})

falcon_rename = {
    'FieldName' : 'name', 
    'tabla'     : 'table',
    'columna'   : 'column',
    'pytype'    : 'pytype', 
    'Size'      : 'len'}

