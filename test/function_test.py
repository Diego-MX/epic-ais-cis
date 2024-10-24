"""funciones de utilidad para llevar a cabo las pruebas unitarias"""

from io import BytesIO
import json

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import pandas as pd
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from toolz import dicttoolz as dtoolz

import config as cfg

spark=SparkSession.builder.getOrCreate()
dbutils=DBUtils(spark)

class FunctionsTest():
    """Clase que contiene todas las fucniones que utilizan las pruebas unitarias"""

    def __init__(self):
        pass

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

# Finite Incatatem