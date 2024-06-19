# pylint: disable=missing-module-docstring
import os

USER_FILE = '../user_databricks.yml'
EPIC_REF = 'gh-1.3'
V_TYPING = '4.7.1'


## Infrastructure resources are defined here.
SETUP_KEYS = {
    'dev': {
        'service-principal': { 
            'client_id'      : 'CLIENT_ID_DEV',             #'QAS_SP_CLIENT',
            'client_secret'  : 'CLIENT_SECRET_DEV',         #'QAS_SP_SECRET',
            'subscription_id': 'SUBSCRIPTION_ID_DEV',       #'QAS_SP_SUBSTN',
            'tenant_id'      : 'TENANT_ID_DEV'}},           #'AAD_TENANT'},
    'qas': {
        'service-principal': { 
            'client_id'      : 'sp-core-events-client',         #'QAS_SP_CLIENT',
            'client_secret'  : 'sp-core-events-secret',         #'QAS_SP_SECRET',
            'subscription_id': 'sp-core-events-subscription',   #'QAS_SP_SUBSTN',
            'tenant_id'      : 'aad-tenant-id'},                #'AAD_TENANT'},
        'databricks-scope': 'eh-core-banking'}, 
    'stg': {
        'service-principal': { # oauth-databricks-qas
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
    'dev': {
        'keyvault' : 'kv-cx-data-qas',
        'storage'  : 'stlakehyliaqas', 
        'storage_path': "ops/fraud-prevention", 
        'blob_path': "ops/fraud-prevention"}, 
    'qas': {
        'keyvault' : 'kv-cx-data-qas',
        'storage'  : 'stlakehyliaqas', 
        'storage_path': "ops/fraud-prevention", 
        'blob_path': "ops/fraud-prevention"}, 
    'stg': {
        'keyvault' : 'kv-cx-data-stg',
        'storage'  : 'stlakehyliastg', 
        'storage_path': "ops/fraud-prevention",
        'blob_path': "ops/fraud-prevention", 
        'storage_paths': ["ops/fraud-prevention", "ops/"]}, 
    'prd': {
        'keyvault' : 'kv-cx-data-prd',
        'storage'  : 'stlakehyliaprd',
        'storage_path': "ops/fraud-prevention", 
        'blob_path': "ops/fraud-prevention"}
}

DBKS_MAPPING = { # Key from Excel Refs, Value on DBKS metastore.
    'clients' : 'star_schema.dim_client', 
    'accounts': 'star_schema.current_account_x'}



ENV = os.getenv('ENV_TYPE')
SERVER = os.getenv('SERVER_TYPE')

