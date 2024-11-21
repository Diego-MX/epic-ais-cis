"""
Programa de pruebas unitarias elementales para el repositorio fraud-prevention
"""

from io import BytesIO
from subprocess import check_call

from azure.core.exceptions import (ClientAuthenticationError,ResourceNotFoundError,
                                   ServiceRequestError,HttpResponseError)
from azure.keyvault.secrets import SecretClient, KeyVaultSecret
from azure.storage.blob import BlobServiceClient

import pandas as pd
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.utils import IllegalArgumentException
import pytest

import config as cfg # pylint: disable = import-error
from function_t import FunctionsTest # pylint: disable = import-error


spark=SparkSession.builder.getOrCreate()
dbutils=DBUtils(spark)

class TestBasic:
    """Pruebas unitarias en las que se prueban las funciones basicas"""

    @pytest.fixture
    def functions_test(self):
        """Llamado de la librería de function_test"""
        return FunctionsTest()

    def test_scope(self,functions_test):
        """Verifica que el SCOPE del respectivo recurso se encuentre disponible"""

        tokener = functions_test.get_user()
        dbks_scope=cfg.SETUP_KEYS[cfg.ENV]["databricks-scope"]
        user_scope=tokener['dbks_scope']
        scopes=dbutils.secrets.listScopes()
        flag_dbks=False
        flag_user=False

        for scope in scopes:
            if scope.name==dbks_scope:
                flag_dbks=True

            if scope.name==user_scope:
                flag_user=True

        assert flag_dbks, "El scope del recurso no existe"
        assert flag_user, "El scope del usuario no existe"

    def test_token_github(self, functions_test):
        """Verifica el token de github mediante la instalación de un recurso"""
        tokener = functions_test.get_user()
        token = dbutils.secrets.get(scope=tokener['dbks_scope'], key=tokener['dbks_token'])

        keys = {
            'url'  : 'github.com/Bineo2/data-python-tools.git', 
            'token': token, 
            'ref'  : "meetme-1"
        }

        argument = "git+https://{token}@{url}@{ref}".format(**keys)
        assert check_call(['pip', 'install', argument])==0,"Fallo el token de GitHub"

    def test_service_principal(self, functions_test):
        """Revisión de permisos para el service principal, se prueba con el 
        el acceso a un recurso"""

        contenedor="gold"
        url="https://stlakehyliaqas.blob.core.windows.net"
        blob_name='ops/fraud-prevention/specs/customers_specs_latest.feather'

        try:

            credencial = functions_test.get_principal()

            b_service = BlobServiceClient(account_url=url, credential=credencial)
            container=b_service.get_container_client(contenedor)
            b_blob = container.get_blob_client(blob_name)
            b_data = b_blob.download_blob()
            b_strm = BytesIO()
            b_data.readinto(b_strm)
            b_strm.seek(0)

            customers_specs = pd.read_feather(b_strm)

            assert customers_specs is not None, "Revisa el service principal que tenga acceso"

        except ResourceNotFoundError as e:
            pytest.fail(f"El blob no existe revisa la dirección que usas: {blob_name}, {e}")

        except IllegalArgumentException as e:
            pytest.fail(f"La credencial no es valida, el secreto no existe en el scope, {e}")

        finally:
            pass

    def test_keyvault(self, functions_test):
        """Esta prueba se lleva meidante una petición a list_properties_of_secrets()
        sí se ejecuta correctamente se accedera al secreto del keyvault.
        """

        keyvault = cfg.AZURE_RESOURCES["qas"]["keyvault"] #cfg.ENV
        url = f"https://{keyvault}.vault.azure.net/"
        d_agent = cfg.SETUP_KEYS["qas"]["service-principal"]
        secret_vault = d_agent["tenant_id"]

        try:

            principal_credential = functions_test.get_principal()
            key_client = SecretClient(url,principal_credential)
            whisper=key_client.list_properties_of_secrets()

            for secrets in whisper:
                if secrets.name == "aad-tenant-id":
                    pass

            assert isinstance(key_client.get_secret(secret_vault),
                KeyVaultSecret),f"A secret with {secret_vault} was not found in this key vault"

        except ResourceNotFoundError as e:
            pytest.fail(f"SecretClient OK, Secret doesnt exist [{keyvault}, {secret_vault}]: {e}")

        except IllegalArgumentException as e:
            pytest.fail(f"La credencial no es valida, el secreto no existe en el scope, {e}")

        except ClientAuthenticationError as e:
            pytest.fail(f"Failed to authenticate SecretClient [{keyvault}]: {e}")

        except ServiceRequestError as e:
            pytest.fail(f"""Failed to establish a new connection:
                        [Errno -2] Name or service not known {e}""")

        except HttpResponseError as e:
            pytest.fail(f"Keyvault no tiene permiso {url} quieres saber más \n {e}")

    def test_tables_exist(self):
        """Verifica si la tabla proveniente de dbks existe"""
        for _, tables_name in cfg.DBKS_MAPPING.items():
            tables_name = cfg.ENV+"."+tables_name
            assert spark.catalog.tableExists(tables_name),f"Tabla no encontrada {tables_name}"

# Finite Incantatem
