"""Pruebas unitarias, se encargan de verificar que todos se encuentre habilitado y funcional"""

from io import BytesIO
import json
from pathlib import Path
from subprocess import check_call
from toolz import dicttoolz as dtoolz

from azure.storage.blob import (BlobServiceClient, BlobClient,ContainerClient)
from azure.core.credentials import AccessToken
from azure.core.exceptions import (ClientAuthenticationError,ResourceNotFoundError,
                                   ServiceRequestError)
from azure.identity import (ClientSecretCredential,DefaultAzureCredential)
from azure.keyvault.secrets import (SecretClient,KeyVaultSecret,KeyVaultSecretIdentifier)
from pyspark.dbutils import DBUtils
from pyspark.sql import (functions, GroupedData, DataFrame,SparkSession)

import pandas as pd
import pytest

import config as cfg

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
dbks_tables = {kk: f"{cfg.ENV}.{tt}"
    for kk, tt in cfg.DBKS_MAPPING.items()}

class Test:
    """Pruebas unitarias de bajo nivel para notebook fraudes"""

    def get_user(self) -> dict:
        """La función obtine la información del usuario por medio de user_databricks"""
        usrs = "../user_databricks.json"

        with open(usrs, 'r',encoding = "utf-8") as file:
            info_json = json.load(file)

        return info_json

    def get_abfss(self) -> str:
        """Se obtiene la dirección delm blob - no esta en uso dado que no se usan los blobs"""
        container = "gold"
        account = cfg.AZURE_RESOURCES[cfg.ENV]["storage"]
        simple_path = cfg.AZURE_RESOURCES[cfg.ENV]["storage-paths"]["fraud"]
        complete_path = f"abfss://{container}@{account}.dfs.core.windows.net/{simple_path}"

        return complete_path

    def get_principal(self) -> dict: # Obtención de credenciales para poder acceder
        """Obtención de la credencial del principal para acceder a otras instancias"""
        agent = cfg.SETUP_KEYS[cfg.ENV]

        dbks_scope = agent["databricks-scope"]
        lam_secret = lambda ss: dbutils.secrets.get(dbks_scope, ss)
        principal = dtoolz.valmap(lam_secret, agent["service-principal"])

        return ClientSecretCredential(**principal)

    def get_root(self) -> str:
        """ Esta función nos brinda el usuario que maneja el código"""
        info_json = self.get_user()
        usr_obj = info_json["user"]
        rt_feather = f"file:/Workspace/Repos/{usr_obj}/fraud-prevention/refs/upload-specs/"
        rt_usr = dbutils.fs.ls(rt_feather)

        return rt_usr

    def get_blob_df(self,container,file_name: str) -> pd.DataFrame:
        """Se accede a un blob para extraer un dataframe """
        add_abfss = self.get_abfss()
        b_blob = container.get_blob_client(add_abfss+"/specs/"+file_name)
        b_data = b_blob.download_blob()
        b_strm = BytesIO()
        b_data.readinto(b_strm)
        b_strm.seek(0)
        data = pd.read_feather(b_strm)

        return data

    def get_storage_client(self, account = None,container = None):
        """Se accede al contenedor"""
        client = self.get_principal()
        account = account or cfg.AZURE_RESOURCES[cfg.ENV]["storage"]
        url = "https://stlakehyliaqas.blob.core.windows.net"
        b_service = BlobServiceClient(url, client)

        return b_service.get_container_client(container)

    def test_scope_dbks(self):
        """Verifica que el SCOPE se encuentre disponible"""
        tokener = self.get_user()
        obj_scope=tokener['dbks_scope']
        scopes=dbutils.secrets.listScopes()
        for scope in scopes:
            if scope.name==obj_scope:
                assert scope.name==obj_scope,"El scope no existe"

    def test_token_dbks(self):
        """Se cerciora que el token funcione correctamente"""
        tokener = self.get_user()
        token = dbutils.secrets.get(scope=tokener['dbks_scope'], key=tokener['dbks_token'])

        keys = {
            'url'  : 'github.com/Bineo2/data-python-tools.git', 
            'token': token, 
            'ref'  : "meetme-1"
        }

        argument = "git+https://{token}@{url}@{ref}".format(**keys)
        assert check_call(['pip', 'install', argument])==0,"Fallo el token"

    def test_principal(self):
        """Revisión de premisos para la credencial obtenida"""
        try:
            client = self.get_principal()
            azure_scope = "api://81bd44f5-7170-4c66-a1d3-dedc2438ac7a/.default"
            token_test = client.get_token(azure_scope)
            assert isinstance(token_test, AccessToken), "Service Principal or Azure Scope Fail."
        except ClientAuthenticationError as e:
            pytest.fail(f"Service Principal cant authenticate: {e}")

    def test_keyvault(self):
        """ Comprueba que la keyvault funcione correctamente - 
        Sigue en investigación se debe de hacer una petición"""
        keyvault = cfg.AZURE_RESOURCES[cfg.ENV]["keyvault"]
        vault_url = f"https://{keyvault}.vault.azure.net/"
        principal_credential = self.get_principal()
        key_client = SecretClient(vault_url,principal_credential)
        d_agent = cfg.SETUP_KEYS[cfg.ENV]["service-principal"]
        secret_vault = d_agent["tenant_id"]

        try:
            assert isinstance(key_client.get_secret(secret_vault),
                KeyVaultSecret),f"A secret with {secret_vault} was not found in this key vault"

        except ResourceNotFoundError as e:
            pytest.fail(f"SecretClient OK, Secret doesnt exist [{keyvault}, {secret_vault}]: {e}")

        except ClientAuthenticationError as e:
            pytest.fail(f"Failed to authenticate SecretClient [{keyvault}]: {e}")

        except ServiceRequestError as e:
            pytest.fail(f"""Failed to establish a new connection:
                        [Errno -2] Name or service not known {e}""")

        # except Exception as e:
        #     pytest.fail(f"Unexpected error with KeyVault, Secret [{keyvault}, {vault}]:\n {e}")

    def test_tables_exist(self):
        """Verifica si la tabla proveniente de dbks existe"""
        for tables_key, tables_name in cfg.DBKS_MAPPING.items():
            tables_name = cfg.ENV+"."+tables_name
            assert spark.catalog.tableExists(tables_name),f"Tabla no encontrada {tables_name}"

    def test_feather_accounts_local(self):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        # Obtención de datos provenientes del archivo feather
        folder = self.get_root()
        data_feather = pd.read_feather(folder[0].path)
        columns_feather = data_feather["columna"].tolist()

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        name_table = cfg.DBKS_MAPPING["accounts"]
        # name_table = cfg.DBKS_MAPPING["clients"]
        data_table = spark.read.table(cfg.ENV+"."+name_table)
        columns_table = data_table.columns

        l_save = []

        for column in columns_feather:
            if column in columns_table:
                l_save.append(column)

        assert len(l_save)==1,"No se encontraron coincidencias en las columnas"

    def test_feather_costumers_local(self):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        # Obtención de datos provenientes del archivo feather
        folder = self.get_root()
        data_feather = pd.read_feather(folder[1].path)
        columns_feather = data_feather["columna"].tolist()

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        name_table = cfg.DBKS_MAPPING["clients"]
        data_table = spark.read.table(cfg.ENV+"."+name_table)
        columns_table = data_table.columns

        l_save = []
        l_special = ["addr_street","addr_external_number","kyc_id","kyc_answer"]

        for column in columns_feather:
            if column in columns_table or column in l_special:
                l_save.append(column)
            elif column == "x_address":
                for i in range(0,2,1):
                    l_save.append(l_special[i])
            elif column in {"x_occupation", "x_src_income"}:
                for i in range(2,4,1):
                    l_save.append(l_special[i])

        assert len(l_save)==18, "No se encontraron coincidencias en las columnas"

    def test_feather_paymonts_local(self):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        # Obtención de datos provenientes del archivo feather
        folder = self.get_root()
        data_feather = pd.read_feather(folder[2].path)
        columns_feather = data_feather["columna"].tolist()

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        # No se cuenta con una tabla símil en databricks

        assert columns_feather!=[],"No se encontro el feather paymonts"

Pruebas = Test()
ACTIVO = Pruebas.get_principal()

# End-of-file (EOF)

