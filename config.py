import os

from epic_py.identity import EpicIdentity


ENV = os.getenv('ENV_TYPE')
SERVER = os.getenv('SERVER_TYPE')

SETUP_KEYS = {
    'qas': {
        'service-principal': { # oauth-databricks-qas
            'client_id'      : 'QAS_SP_CLIENT',
            'client_secret'  : 'QAS_SP_SECRET', 
            'subscription_id': 'QAS_SP_SUBSTN', 
            'tenant_id'      : 'AAD_TENANT'}, 
    }
}

AZURE_RESOURCES = {
    'qas': {
        'keyvault' : 'kv-cx-data-qas',
        'storage'  : 'stglakehyliaqas', 
        'blob_path': "ops/fraud-prevention"}
}


app_agent = EpicIdentity.create(server=SERVER, config=SETUP_KEYS[ENV])
app_resourcer = app_agent.setup_resourcer(AZURE_RESOURCES[ENV])

blob_path = AZURE_RESOURCES[ENV]['blob_path']






