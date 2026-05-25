"""
Pruebas unitarias setup, 
Estas pruebas tienen como finalidad de revisar que los insumos especificos del repositorio
se encuentren habilitados y listos para ser utilizados.
"""

<<<<<<< HEAD:tests/test_dbks.py
from io import BytesIO
import json

from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.credentials import AccessToken
from azure.core.exceptions import (ClientAuthenticationError, ResourceNotFoundError, 
    ServiceRequestError)
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient, KeyVaultSecret
import pandas as pd
=======
import warnings

>>>>>>> dev-juan:test/test_setup.py
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import pytest
<<<<<<< HEAD:tests/test_dbks.py
from toolz import dicttoolz as dtoolz

from epic_py.delta import EpicDF
import config as cfg
from src import dbks_tables, app_path, app_abfss
from tests import ENV
=======

from function_t import FunctionsTest # pylint:disable = import-error
import config as cfg # pylint: disable = import-error
import config_t as cfg_t # pylint: disable = import-error

warnings.filterwarnings("ignore", category=DeprecationWarning)
>>>>>>> dev-juan:test/test_setup.py

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class TestRepository:
    """Pruebas unitarias correspondientes al repositorio"""

<<<<<<< HEAD:tests/test_dbks.py
class TestConfig:
    """Pruebas unitarias de bajo nivel para notebook fraudes"""
    def get_principal(self): # Obtención de credenciales para poder acceder
        """Obtención de la credencial del principal para acceder a otras instancias"""
        agent = cfg.SETUP_KEYS[ENV]
        dbks_scope = agent["databricks-scope"]
        lam_secret = lambda ss: dbutils.secrets.get(dbks_scope, ss)
        principal = dtoolz.valmap(lam_secret, agent["service-principal"])
        return ClientSecretCredential(**principal)

    def get_usr(self):
        """ Esta función nos brinda el usuario que maneja el código"""
        usrs = "./user_databricks.json"
        with open(usrs, 'r', encoding = "utf-8") as file:
            informacion_json = json.load(file)
        usr_obj = informacion_json["user"]
        rt_feather = f"file:/Workspace/Repos/{usr_obj}/fraud-prevention/refs/upload-specs/"
        rt_usr = dbutils.fs.ls(rt_feather)
        return rt_usr

    def get_blob_df(self,container,file_name: str) -> pd.DataFrame:
        """Se accede a un blob para extraer un dataframe """
        b_blob = container.get_blob_client(app_path+"/specs/"+file_name)
        b_data = b_blob.download_blob()
        b_strm = BytesIO()
        b_data.readinto(b_strm)
        b_strm.seek(0)
        data = pd.read_feather(b_strm)
        return data

    def get_storage_client(self, account = None,container = None):
        """Se accede al contenedor"""
        client = self.get_principal()
        account = account or cfg.AZURE_RESOURCES[ENV]["storage"]
        the_url = "https://stlakehyliaqas.blob.core.windows.net"
        b_service = BlobServiceClient(the_url, client)
        return b_service.get_container_client(container)

    def test_principal(self):
        """Revisión de premisos para la credencial obtenida"""
        try:
            client = self.get_principal()
            azure_scope = "api://81bd44f5-7170-4c66-a1d3-dedc2438ac7a/.default"
            token_test = client.get_token(azure_scope)
            assert isinstance(token_test, AccessToken), "Service Principal or Azure Scope Fail."
        except ClientAuthenticationError as e:
            pytest.fail(f"Service Principal cant authenticate: {e}")
        # except Exception as e:
        #     pytest.fail(f"Error with Service Principal's token: {e}")

    def test_keyvault(self):
        """ Comprueba que la keyvault funcione correctamente - 
        Sigue en investigación se debe de hacer una petición"""
        keyvault = cfg.AZURE_RESOURCES[ENV]["keyvault"]
        vault_url = f"https://{keyvault}.vault.azure.net/"
        principal_credential = self.get_principal()
        key_client = SecretClient(vault_url,principal_credential)
        d_agent = cfg.SETUP_KEYS[ENV]["service-principal"]
        vault = d_agent["tenant_id"]

        try:
            assert isinstance(key_client.get_secret(vault),
                KeyVaultSecret),f"A secret with {vault} was not found in this key vault"

        except ResourceNotFoundError as e:
            pytest.fail(f"SecretClient OK, Secret doesnt exist [{keyvault}, {vault}]: {e}")

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
            tables_name = ENV+"."+tables_name
            assert spark.catalog.tableExists(tables_name),f"Tabla no encontrada {tables_name}"

    def test_feather_exist(self):
        """Comprueba sí el feather existe"""
        folder = dbutils.fs.ls(app_abfss)

        assert folder != [],"No hay archivos en la carpeta"
        for file in folder:
            assert file.name.endswith('cols.feather'), "No es un archivo feather!"

    def test_specs_content(self):
        """Revisa que el feather contenido en la carpeta specs contenga información"""
        container =self.get_storage_client(None,"gold")
        folder = dbutils.fs.ls(app_abfss+"/specs") # ruta que apunta a la carpeta specs
        # folder = dbutils.fs.ls(app_abfss)
        # # ruta que apunta a la carpeta fraud-prevention, las dos son validas
        for file in folder:
            if file.name.endswith("latest.feather"):
                data = self.get_blob_df(container,file.name)
                assert not data.empty, "El blob se encuentra vacío 😱"

    def test_specs_exist(self):
        """# Constata que el feather exista en la carpeta specs"""
        folder = dbutils.fs.ls(app_abfss+"/specs")
        container = self.get_storage_client(None,"gold")

        for file in folder:
            if file.name.endswith("latest.feather"):
                b_blob = container.get_blob_client(app_path+"/specs/"+file.name)
                assert isinstance(b_blob,BlobClient), "No es cliente del contenedor"

    def test_feather_accounts(self):
=======
    @pytest.fixture
    def import_function(self):
        "Llamado de la clase FunctionsTest"
        return FunctionsTest()

    def test_feather_accounts_local(self,import_function):
>>>>>>> dev-juan:test/test_setup.py
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        # Obtención de datos provenientes del archivo feather
        user = import_function.get_user_note()
        data_feather = pd.read_feather(f"file:/Workspace/Repos/{user}"
                                       f"/{cfg_t.PATHS[cfg.ENV]}/refs/upload-specs/accounts_cols.feather")
        columns_feather = data_feather["columna"].tolist()

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        name_table = cfg.DBKS_MAPPING["accounts"]
        data_table = spark.read.table(cfg.ENV+"."+name_table)
        columns_table = data_table.columns

        l_save = []

        for column in columns_feather:
            if column in columns_table:
                print(column)
                l_save.append(column)

        assert len(l_save)==2,"No se encontraron coincidencias en las columnas en AccountsLocal"

    def test_feather_costumers_local(self,import_function):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        user = import_function.get_user_note()
        # Obtención de datos provenientes del archivo feather
        data_feather = pd.read_feather(f"file:/Workspace/Repos/{user}"
                                       f"/{cfg_t.PATHS[cfg.ENV]}//refs/upload-specs/customers_cols.feather")
        columns_feather = data_feather["columna"].tolist()

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        name_table = cfg.DBKS_MAPPING["clients"]
        data_table = spark.read.table(cfg.ENV+"."+name_table)
        columns_table = data_table.columns

        l_save = []
        l_special = ["addr_street","addr_external_number","kyc_id","kyc_answer"]

<<<<<<< HEAD:tests/test_dbks.py
        for key, _ in d_feathers.items():
            if key != "payments":
                for item in d_feathers[key]:
                    if item == "None" or item == "N/A":
                        pass
                    elif item in d_tables[key] or item in l_special:
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
=======
        for column in columns_feather:
            if column in columns_table or column in l_special:
                l_save.append(column)
            elif column == "x_address":
                for i in range(0,2,1):
                    l_save.append(l_special[i])
            elif column in {"x_occupation", "x_src_income"}:
                for i in range(2,4,1):
                    l_save.append(l_special[i])
>>>>>>> dev-juan:test/test_setup.py

        assert len(l_save)==15, "No se encontraron coincidencias en las columnas"

    def test_feather_paymonts_local(self,import_function):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        user = import_function.get_user_note()
        # Obtención de datos provenientes del archivo feather
        data_feather = pd.read_feather(f"file:/Workspace/Repos/{user}"
                                       f"/{cfg_t.PATHS[cfg.ENV]}//refs/upload-specs/payments_cols.feather")
        columns_feather = data_feather["columna"].tolist()

<<<<<<< HEAD:tests/test_dbks.py
class TestEpicPakcage: 
    pass 

=======
        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        # No se cuenta con una tabla símil en databricks

        assert columns_feather!=[],"No se encontro el feather paymonts"

# Finite Incantatem
>>>>>>> dev-juan:test/test_setup.py
