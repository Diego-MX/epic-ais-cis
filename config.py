# pylint: disable=anomalous-backslash-in-string
"""
En este script se inicializan variables ligadas a objetos que se implementan por 
parte del equipo de Infraestructura.  

En particular las llaves de secretos en llaveros de Azure y Databricks admiten la 
regex '[a-z\-]*'. 

La correspondencia con variables en '.env' es mediante: 
{azure|databricks} -> UPPER -> SUB(-, _) -> {.env}
"""

REQS_FILE = 'pip_reqs.txt'
USER_FILE = 'user_databricks.json'


## Infrastructure resources are defined here.
SETUP_KEYS = {
    'dev': {
        'service-principal': { 
            'client_id'      : 'CLIENT_ID_DEV',                 #'QAS_SP_CLIENT',
            'client_secret'  : 'CLIENT_SECRET_DEV',             #'QAS_SP_SECRET',
            'subscription_id': 'SUBSCRIPTION_ID_DEV',           #'QAS_SP_SUBSTN',
            'tenant_id'      : 'TENANT_ID_DEV'}},               #'AAD_TENANT'},
    'qas': {
        'databricks-scope': 'eh-core-banking',
        'github-access': 'github-access-token', 
        'service-principal': { 
            'client_id'      : 'sp-core-events-client',         #'QAS_SP_CLIENT',
            'client_secret'  : 'sp-core-events-secret',         #'QAS_SP_SECRET',
            'subscription_id': 'sp-core-events-subscription',   #'QAS_SP_SUBSTN',
            'tenant_id'      : 'aad-tenant-id'}},               #'AAD_TENANT'},
    'stg': {
        'service-principal': {      # oauth-databricks-qas
        'databricks-scope': 'eh-core-banking', 
        'github-access': 'github-access-token', 
            'client_id'      : 'sp-core-events-client',         #'QAS_SP_CLIENT',
            'client_secret'  : 'sp-core-events-secret',         #'QAS_SP_SECRET', 
            'subscription_id': 'sp-core-events-suscription',    #'QAS_SP_SUBSTN', 
            'tenant_id'      : 'aad-tenant-id'}},               #'AAD_TENANT'}, 
    'prd': {
        'github-access': 'github-access-token', 
        'databricks-scope': 'cx-collections',  #   eh-core-banking
        'service-principal': {
            'client_id'      : 'sp-collections-client', # 
            'client_secret'  : 'sp-collections-secret', #          
            'subscription_id': 'sp-collections-subscription', # 
            'tenant_id'      : 'aad-tenant-id'}},
}

AZURE_RESOURCES = {
    'qas': {
        'keyvault' : 'kv-cx-data-qas',
        'storage'  : 'stlakehyliaqas', 
        'storage-paths': {
            'fraud': "ops/fraud-prevention", 
            'core-banking': "ops/core-banking-x/current-account", 
        'metastore': {  
            'server': 'sqlserver-lakehylia-data-qas', 
            'database': 'lakehylia_metastore_qas', 
            'user': 'sqlAdministratorLoginUserMetastore', 
            'password': 'sqlAdministratorLoginPwdMetastore'}
        }}, 
    'stg': {
        'keyvault' : 'kv-cx-data-stg',
        'storage'  : 'stlakehyliastg', 
        'storage-paths': {
            'fraud': "ops/fraud-prevention", 
            'core-banking': "ops/core-banking-x/current-account"
        }},  
    'prd': {
        'keyvault' : 'kv-cx-data-prd',
        'storage'  : 'stlakehyliaprd',
        'storage-paths': {
            'fraud': "ops/fraud-prevention", 
            'core-banking': "ops/core-banking-x/current-account"
} } }

DBKS_MAPPING = { # Key from Excel Refs, Value on DBKS metastore.
    'clients' : 'star_schema.dim_client',  # 
    'accounts': 'star_schema.current_account_x'}

