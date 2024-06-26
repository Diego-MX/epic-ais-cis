# libreria Establecida por Databricks azure

from azure.storage.blob import (BlobServiceClient,
                                BlobClient,
                                ContainerClient)
from azure.core.credentials import AccessToken
from azure.core.exceptions import (ClientAuthenticationError, 
                                    ResourceNotFoundError, 
                                    ServiceRequestError)
from azure.identity import (ClientSecretCredential, 
                            DefaultAzureCredential)
from azure.keyvault.secrets import (SecretClient, 
                                    KeyVaultSecret,
                                    KeyVaultSecretIdentifier) 
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# libreria de python

from io import BytesIO
import os
import pandas as pd
import pytest
from pathlib import Path
from toolz import dicttoolz as dtoolz
from yaml import safe_load

# Librerias personales
import config as cfg 
import dependencies as deps
# deps.gh_epicpy('meetme-1', 
#     tokenfile='../user_databricks.yml', typing=False, verbose=True) # <----
from epic_py.delta import EpicDF, EpicDataBuilder # <----
from epic_py.platform import AzureResourcer

from src import (app_agent, app_resourcer,
                dbks_tables, app_path, app_abfss)
   
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class Test:
    def get_principal(self): # Obtención de credenciales para poder acceder
        agent = cfg.SETUP_KEYS[cfg.ENV]
        dbks_scope = agent["databricks-scope"]
        λ_secret = lambda ss: dbutils.secrets.get(dbks_scope, ss)
        principal = dtoolz.valmap(λ_secret, agent["service-principal"])
        return ClientSecretCredential(**principal)
    
    def get_usr(self): # Esta función nos brinda el usuario que maneja el código
        usrs = "../user_databricks.yml"

        with open(usrs, 'r') as _f:
            tkn = safe_load(_f)

        usr_obj = tkn["user"]
        rt_feather = f"file:/Workspace/Repos/{usr_obj}/fraud-prevention/refs/upload-specs/"
        rt_usr = dbutils.fs.ls(rt_feather)

        return rt_usr

    def get_blob(self,container,file_name: str) -> pd.DataFrame():
        b_blob = container.get_blob_client(app_path+"/specs/"+file_name)
        b_data = b_blob.download_blob()
        b_strm = BytesIO()
        b_data.readinto(b_strm)
        b_strm.seek(0)
        data = pd.read_feather(b_strm)

        return data

    def get_storage_client(self, account = None,container = None,info = False):
        client = self.get_principal()
        account = account or cfg.AZURE_RESOURCES[cfg.ENV]["storage"]
        print(account)
        the_url = AzureResourcer.get_resource_url('blob', account)
        print(the_url)
        b_service = BlobServiceClient(the_url, client)
        print(b_service)
        # if container: 
        #     return b_service.get_container_client(container)
        # if info:  # pylint: disable=no-else-return
        #     return b_service.get_account_information()
        # else:  
        #     return b_service

    def test_principal(self): # Revisión de premisos para la credencial obtenida
        try: 
            client = self.get_principal()
            azure_scope = "api://81bd44f5-7170-4c66-a1d3-dedc2438ac7a/.default"#cfg.SETUP_KEYS[cfg.ENV]['databricks-scope']
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

        vault = D_agent["tenant_id"]

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
    
    def test_tbl_exist(self):
        for tbl_key, tbl_name in cfg.DBKS_MAPPING.items():
            tbl_name = cfg.ENV+"."+tbl_name
            assert spark.catalog.tableExists(tbl_name),f"Tabla no encontrada {tbl_name}"
        
        return

    def test_feather_exist(self): # Comprueba sí el feather existe
        folder = dbutils.fs.ls(app_abfss)

        if folder == []:
            assert 1 == 2,"No hay archivos en la carpeta"
            return

        for file in folder:
            if file.name.endswith("cols.feather"):
                assert file.name.endswith('cols.feather'), "No es un archivo feather!"

        return 

    def test_blob_content(self): # Revisa que el feather contenga información
        container = app_resourcer.get_storage_client(None,"gold")
        folder = dbutils.fs.ls(app_abfss+"/specs") # ruta que apunta a la carpeta specs 
        # folder = dbutils.fs.ls(app_abfss) # ruta que apunta a la carpeta fraud-prevention, las dos son validas 
        
        for file in folder:
            if file.name.endswith("latest.feather"):
                data = self.get_blob(container,file.name)
                assert data.empty != True, "El blob se encuentra vacío 😱"

        return
    
    def test_blob_exist(self): # Constata que el blob exista
        folder = dbutils.fs.ls(app_abfss+"/specs")
        container = app_resourcer.get_storage_client(None,"gold")

        for file in folder:
            if file.name.endswith("latest.feather"):
                b_blob = container.get_blob_client(app_path+"/specs/"+file.name)
                assert isinstance(b_blob,BlobClient), "No es cliente del contenedor"

        return

    def test_feather_col(self):
        folder = dbutils.fs.ls(app_abfss+"/specs")
        container = app_resourcer.get_storage_client(None,"gold")
        D_feathers = {}; D_tbl = {}

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        for tbl_key, tbl_name in cfg.DBKS_MAPPING.items():
            tbl = (EpicDF(spark, dbks_tables[tbl_key]))
            D_tbl[tbl_key] = tbl.columns

        # Extracción del nombre de las columnas de los feathers. Veinen de EXCEL
        for file in folder:
            if file.name.endswith("latest.feather"):
                data = self.get_blob(container,file.name)
                sep = file.name.split("_")

                if "customers" in sep:
                    key = "clients" 
                else:
                    key = sep[0]

                columns = data["columna"]
                D_feathers[key] = columns.tolist()

        l_save = []
        l_keep = []
        l_special = ["addr_street","addr_external_number","kyc_id","kyc_answer"]

        for key,items in D_feathers.items():
            if key != "payments":
                for item in D_feathers[key]:
                    if item == "None" or item == "N/A":
                        pass
                    
                    elif item in D_tbl[key] or item in l_special:
                        l_save.append(item)
                    
                    elif item == "x_address":
                        for i in range(0,2,1):
                            l_save.append(l_special[i])
                    
                    elif item == "x_occupation" or item == "x_src_income":
                            for i in range(2,4,1):
                                l_save.append(l_special[i])
                    
                    else:
                        pass
                    
                l_keep.append(l_save)
                l_save = []
        
        duty = [1,18]

        for i in range(0,len(l_keep),1):
            assert len(l_keep[i])==duty[i], "Fallo en las columnas"

        return 



Test = Test()
Activo = Test.get_storage_client()
print(Activo)
