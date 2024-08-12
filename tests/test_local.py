""" libreria Establecida por Databricks azure """

from io import BytesIO
import os

from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AccessToken
from azure.core.exceptions import (ClientAuthenticationError, ResourceNotFoundError, 
    ServiceRequestError)
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient, KeyVaultSecret
import pandas as pd
import pytest
from toolz import dicttoolz as dtoolz

from epic_py.platform import EpicIdentity
from epic_py.platform.identity import LocalIdentity
from src import app_path

import config as cfg
from . import ENV, SERVER


class TestConfig:
    """Pruebas unitarias de bajo nivel para notebook fraudes"""
    def get_principal(self): # Obtención de credenciales para poder acceder
        """Obtención de la credencial del principal para acceder a otras instancias"""
        agent = cfg.SETUP_KEYS[ENV]
        lam_secret = lambda ss: os.getenv(ss.upper().replace('-', '_'))
        principal = dtoolz.valmap(lam_secret, agent["service-principal"])
        return ClientSecretCredential(**principal)


    def get_blob_df(self, container, file_name:str) -> pd.DataFrame:
        """Se accede a un blob para extraer un dataframe """
        b_blob = container.get_blob_client(app_path+"/specs/"+file_name)
        b_data = b_blob.download_blob()
        b_strm = BytesIO()
        b_data.readinto(b_strm)
        b_strm.seek(0)
        return pd.read_feather(b_strm)
    

    def get_storage_client(self, account=None, container=None):
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
            assert isinstance(token_test, AccessToken),\
                "Service Principal or Azure Scope Fail."
        except ClientAuthenticationError as e:
            pytest.fail(f"Service Principal cant authenticate: {e}")


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


class TestEpicPackage: 
    def test_agent(self):
        assert SERVER == 'local',\
            f"Testing SERVER variable {SERVER} != 'local'"
        t_agent = EpicIdentity.create('local', cfg.SETUP_KEYS[ENV])

        assert isinstance(t_agent, LocalIdentity),\
            "El agente t-agent no se creó con clase LocalIdenity."

        t_resources = t_agent.get_resourcer(cfg.AZURE_RESOURCES[ENV])
        assert 'storage' in t_resources,\
            "No se guardó el Storeage"




