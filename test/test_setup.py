# libreria Establecida por Databricks azure

import azure as azure # <----
from azure.core.credentials import AccessToken
from azure.core.exceptions import (ClientAuthenticationError, 
                                    ResourceNotFoundError, 
                                    ServiceRequestError)
from azure.identity import (ClientSecretCredential, 
                            DefaultAzureCredential)
from azure.keyvault.secrets import (SecretClient, 
                                    KeyVaultSecret,
                                    KeyVaultSecretIdentifier) 
# from azure.storage.blob import BlobServiceClient,ContainerClient # <----
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# print(dir(azure.keyvault.secrets.KeyVaultSecretIdentifier.name.fget)) # <----

# libreria de python

import os
import pandas as pd
import pytest
from pathlib import Path
from toolz import dicttoolz as dtoolz
from yaml import safe_load

# print(dir(os),os.getcwd()) # <----

# Librerias personales
import config as cfg 
import dependencies as deps
# deps.gh_epicpy('meetme-1', 
#     tokenfile='../user_databricks.yml', typing=False, verbose=True) # <----
# from epic_py.delta import EpicDF, EpicDataBuilder # <----
from src import (app_agent, app_resourcer,
                blob_path, dbks_tables, 
                falcon_handler, falcon_rename)

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class Test:
    def get_principal(self): # Optención de credenciales para poder acceder
        agent = cfg.SETUP_KEYS[cfg.ENV]
        dbks_scope = agent["databricks-scope"]
        λ_secret = lambda ss: dbutils.secrets.get(dbks_scope, ss)
        principal = dtoolz.valmap(λ_secret, agent["service-principal"])
        return ClientSecretCredential(**principal)
    
    def get_feathers(self):
        usrs = "../user_databricks.yml"

        with open(usrs, 'r') as _f:
            tkn = safe_load(_f)

        usr_obj = tkn["user"]
        rt_feather = f"file:/Workspace/Repos/{usr_obj}/fraud-prevention/refs/upload-specs/"
        rt_usr = dbutils.fs.ls(rt_feather)

        return rt_usr

    def test_principal(self): # Revisión de premisos para la credencial obtenida
        try: 
            client = self.get_principal()
            azure_scope = cfg.AZURE_RESOURCES[cfg.ENV]['api-scope']
            token_test = client.get_token(azure_scope)
            assert isinstance(token_test, AccessToken), "Service Principal or Azure Scope Fail."
        except ClientAuthenticationError as e: 
            pytest.fail(f"Service Principal cant authenticate: {e}")
        except Exception as e: 
            pytest.fail(f"Error with Service Principal's token: {e}")
        return

    def test_keyvault(self): # Sigue en investigación 
        keyvault = cfg.AZURE_RESOURCES[cfg.ENV]["keyvault"]
        vault_url = f"https://{keyvault}.vault.azure.net/"
        principal_credential = self.get_principal()
        key_client = SecretClient(vault_url,principal_credential)
        D_agent = cfg.SETUP_KEYS[cfg.ENV]["service-principal"]

        for key, vault in D_agent.items():
            try:
                assert isinstance(key_client.get_secret(vault),KeyVaultSecret), f"A secret with {vault} was not found in this key vault"

            except ResourceNotFoundError as e:
                pytest.fail(f"SecretClient OK, Secret doesnt exist [{keyvault}, {vault}]: {e}")
                pass

            except ClientAuthenticationError as e:
                pytest.fail(f"Failed to authenticate SecretClient [{keyvault}]: {e}")
                pass

            except ServiceRequestError as e:
                pytest.fail(f"Failed to establish a new connection: [Errno -2] Name or service not known {e}")
                pass
    
            except Exception as e:
                pytest.fail(f"Unexpected error with KeyVault, Secret [{keyvault}, {vault}]:\n {e}")
                pass

        return 

    def test_feather(self):
        rt_usr = self.get_feathers()

        if rt_usr == []:
            assert 1 == 2,"No hay archivos en la carpeta"
            return

        for file in rt_usr:
            assert file.name.endswith('.feather'), "No es un archivo feather!"

        return 

    # def test_feather_col(self): # Falta hacer la compración, ya estan los nombres de las columnas disponibles para poder utilizarlos

    #     feathers = self.get_feathers()

    #     D_feathers = {}; D_tbl = {}

    #     for tbl_key, tbl_name in cfg.DBKS_MAPPING.items():
    #         tbl = (EpicDF(spark, dbks_tables['accounts']))
    #         D_tbl[tbl_key] = tbl.columns

    #     for file in feathers:
    #         feathers_review = pd.read_feather(file.path)
    #         sep = file.name.split("_")

    #         if "customers" in sep:
    #             key = "client"
            
    #         else:
    #             key = sep[0]

    #         columns = feathers_review["FieldName"]
    #         D_feathers[key] = columns.tolist()

        
    #     # print(D_tbl)
    #     # print("")
    #     # print(D_feathers)
    #     assert 5 == 5,"Fallo" 
    #     return 

    def test_tbl_exist(self):
        for tbl_key, tbl_name in cfg.DBKS_MAPPING.items():
            tbl_name = cfg.ENV+"."+tbl_name
            assert spark.catalog.tableExists(tbl_name),f"Tabla no encontrada {tbl_name}"

# Test = Test()
# Activo = Test.test_tbl_exist()
# print(Activo)