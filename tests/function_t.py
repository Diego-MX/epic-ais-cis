"""Funciones de utilidad para llevar a cabo las pruebas unitarias, este script
no cuenta con pruebas unitarias sólo contiene funciones que facilitan las pruebas
unitarias"""

from io import BytesIO
import json

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import pandas as pd
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, DataFrame
from toolz import dicttoolz as dtoolz

import config as cfg # pylint: disable = import-error

spark=SparkSession.builder.getOrCreate()
dbutils=DBUtils(spark)

class FunctionsTest():
    """Clase que contiene todas las fucniones que utilizan las pruebas unitarias"""

    def __init__(self):
        pass

    def get_user_gh(self) -> dict:
        """La función obtine la información del usuario por medio de user_databricks"""
        usrs = "../user_databricks.json"

        with open(usrs, 'r',encoding = "utf-8") as file:
            info_json = json.load(file)

        return info_json

    def get_user_note(self) -> dict:
        """La función obtine la información del usuario por medio de user_databricks"""
        user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
        return user

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
        lam_secret = lambda ss: dbutils.secrets.get(dbks_scope, ss) # pylint: disable = unnecessary-lambda-assignment
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
        """Se accede a un blob para extraer un dataframe 
        :param container: Nombre del contenedor con el que se trabaja
        :param file_name: Nombre del archivo que se desea extraer del blob
        """
        add_abfss = self.get_abfss()
        b_blob = container.get_blob_client(add_abfss+"/specs/"+file_name)
        b_data = b_blob.download_blob()
        b_strm = BytesIO()
        b_data.readinto(b_strm)
        b_strm.seek(0)
        data = pd.read_feather(b_strm)

        return data

    def get_storage_client(self, account = None,container = None):
        """Se accede al contenedor
        :param account: Nombre de la cuenta, ya sea la que se encuentra en 
            la configuración o la que se ingresa de forma manual
        :param container: Nombre del contenedor con el que se desea trabajar
        """
        client = self.get_principal()
        account = account or cfg.AZURE_RESOURCES[cfg.ENV]["storage"]
        url = "https://stlakehyliaqas.blob.core.windows.net"
        b_service = BlobServiceClient(url, client)

        return b_service.get_container_client(container)

    def response_retcode(self,response:int)->None:
        """Función que ayuda a visualizar el resultado de las pruebas 
        unitarias, permite un mejor entendimiento de lo que ocurre en las
        pruebas.
        :param response: Es la respuesta que devuelven las pruebas unitarias
            al finalizar. 
        """
        if response == 0:
            print("Éxito, todas las pruebas fueron exitosas")

        elif response == 1:
            assert response != 1, "Fallos en las pruebas"

        elif response == 2:
            assert response != 2, "Error durante la ejecución"

        elif response == 3:
            assert response != 3, "Interrupción de la ejecución"

        elif response == 4:
            assert response != 4, "Error de uso o de instalación de modulos"

        else:
            print("Este error no ha sido mapeado, investigalo y documentalo")

    def columns_extract(self,repo:dict) -> None:
        """La función extrae los nombres de las columnas, estos son obtenidos del
        diccionario de config-t.
        : param repo: Diccionario con los nombres de las tablas y sus ubicaciones"""

        user = self.get_user()["user"]
        columns_tables = {"fraud_prevention":{"columns":{},
                                            "data_type":{}}}

        for key, content in repo.items():
            for table in content:
                df_table = spark.read.table(f"{cfg.ENV}.{key}.{table}")
                ls_columns_table = df_table.columns
                columns_tables["fraud_prevention"]["columns"][table] = ls_columns_table
                columns_tables["fraud_prevention"]["data_type"][table] = dict(df_table.dtypes)

        with open(f"/Workspace/Repos/{user}"
                  "/fraud-prevention/test/archive/fraud_prevention_columns.json"
                  ,"w", encoding="utf-8") as columns:
            json.dump(columns_tables,columns,indent=4)

    def read_json(self)->dict:
        """La función realiza la extracción de los datos del archivo json 
        para la comparación con los nuevos datos
        """
        user = self.get_user()["user"]
        archive = (f"/Workspace/Repos/{user}"
                   "/fraud-prevention/test/archive/fraud_prevention_columns.json")
        with open(archive,"r", encoding="utf-8") as columns:
            dict_json = json.load(columns)

        return dict_json

    def deferences_columns(self, reference_1:list, table:str, reference_2:list)->str:
        """Muestra la diferencia entre las columnas de las tablas, además meustra un 
        pequeño reporte en el cual se da información acerca del incidente
        :param reference_1: Se ingresa la lista del nombre de las columnas del 
            archivo evaluador 
        :param table: Nombre de la tabla en cuestión
        :param reference_2: Lista con el nombre de las columnas de las tablas que son 
            evaluados.
        """

        deference = list(set(reference_1[table])-set(reference_2))
        report = ""

        for column in deference:
            if column in reference_1[table]:
                complement = "evaluadora"
            elif column in reference_2:
                complement = "evaluada"
            else:
                print("No existe esta opción, extraño si sale")
                complement = "error"
                column = "error"

            letter = (f"En la tabla '{table}' la cual es '{complement}', se encuentra"
                        " la columna '{column}' la cual crea diferencias")
            report = report+letter+"\n"

        return report

    def deferences_types(self, reference_1: dict, table: str, reference_2: DataFrame):
        """Muestra la diferencia de los tipos de datos de las columnas de las tablas
         en cuestión muestra la columna con diferencias y los tipos de los dos archivos,
        por ultimo muestra esta información
        :param reference_1: Diccionario de los tipos de datos del archivo referencia
        :param table: Nombre de la tabla en cuestión, con el se puede acceder al
            archivo de referencia y a la tabla evaluada
        :param reference_2: DataFrame de la tabla evaluada
        """
        report = ""
        deference = list(reference_1[table].items() ^ dict(reference_2.dtypes).items()) # pylint: disable = unused-variable
        letter = (
            f"Se encontro la siguiente diferencia en el tipo de dato de la columna "        
            f"*{deference[0][0]}* en archive marca *{deference[0][1]}*, "
            f"en el actual es *{deference[1][1]}*"
        )
        report = report + letter + "\n"

        return report

# Finite Incantatem
