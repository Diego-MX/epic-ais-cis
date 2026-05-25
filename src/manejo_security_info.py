# Databricks notebook source
# MAGIC %md
# MAGIC # Manejo de Archivos 

# COMMAND ----------

# MAGIC %md
# MAGIC > El código tiene la función, de acomodar los archivos que se encuentran en la ruta `dbfs:/user/hive/warehouse/default` al **blob** correspondiente. En este caso se manejan los archivos de relevancia para el código `info_security.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Librerias

# COMMAND ----------

from datetime import datetime, timedelta
import io

from azure.identity import ClientSecretCredential
from azure.storage.blob import (BlobServiceClient, BlobClient,ContainerClient)
import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from toolz import dicttoolz as dtoolz

import config as cfg

# COMMAND ----------

spark=SparkSession.builder.getOrCreate()
dbutils=DBUtils(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingredientes
# MAGIC

# COMMAND ----------

account = cfg.AZURE_RESOURCES[cfg.ENV]["storage"]
agent = cfg.SETUP_KEYS[cfg.ENV]
container = "gold"
dbks_scope = agent["databricks-scope"]
files=["accounts_cols/","customers_cols/","payments_cols/"]
original_path=dbutils.fs.ls("dbfs:/user/hive/warehouse")
service=agent["service-principal"]
simple_path = cfg.AZURE_RESOURCES[cfg.ENV]["storage-paths"]["fraud"]
url = "https://stlakehyliaqas.blob.core.windows.net"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Tras bambalinas

# COMMAND ----------

class Sabe():
    def __init__(self, contenedor:str, cuenta:str, 
                dir_simple:str, url:str, 
                agente:dict, scope:str, servicio:dict)->None:
        
        self.contenedor=contenedor
        self.cuenta=cuenta
        self.dir_simple=dir_simple
        self.url=url
        self.agent=agent
        self.scope=scope
        self.servicio=servicio

        self._credencial()

    def subida_datos(self,archivo:str,unix:int)->None:
        path=f"dbfs:/user/hive/warehouse/{archivo}_cols/"

        df = (spark.read.format("delta")
              .load(path, header=True, inferSchema=True))
        
        pandas_df = df.toPandas()
        feather_buffer = io.BytesIO()
        feather.write_feather(pandas_df, feather_buffer)
        feather_buffer.seek(0)
        tiempo=self._unix_a_datetime(unix)
        blob_name_0=f"ops/fraud-prevention/specs/{archivo}_specs_latest.feather"

        reservorio=self._cliente_blob()
        reservorio.upload_blob(blob_name_0, feather_buffer, overwrite=True)

        pandas_df = df.toPandas()
        feather_buffer = io.BytesIO()
        feather.write_feather(pandas_df, feather_buffer)
        feather_buffer.seek(0)
        tiempo=self._unix_a_datetime(unix)
        blob_name_1=f"ops/fraud-prevention/specs/{archivo}{tiempo}.feather"

        reservorio.upload_blob(blob_name_1, feather_buffer, overwrite=True)

        print(f"Archivo {archivo} listo!")

    def eliminacion_datos(self,blob_name:str)->None:
        reservorio=self._cliente_blob()
        reservorio.delete_blob(blob_name)
        print(f"Archivo {blob_name} eliminado")

    def mover_y_renombrar_archivo(self, archivo_origen: str, nuevo_nombre: str) -> None:

        df = spark.read.parquet(archivo_origen)
        nuevo_path = f"{self.dir_simple}/{nuevo_nombre}"
        df.write.mode("overwrite").parquet(f"wasbs://{self.contenedor}@{self.cuenta}.blob.core.windows.net/{nuevo_path}")
        print(f"Archivo movido y renombrado exitosamente a {nuevo_path}.")

    def _credencial(self):
        d_secret=lambda name: dbutils.secrets.get(self.scope,name)
        principal = dtoolz.valmap(d_secret, self.servicio)
        self.credencial=ClientSecretCredential(**principal)
        print("Credencial lista!")

    def _cliente_blob(self):
        b_service = BlobServiceClient(self.url, self.credencial)
        reservorio = b_service.get_container_client(self.contenedor)
        return reservorio

    def _unix_a_datetime(self,unix:int, horario:int = 6)->str:
        milisegundos=1000000000000 

        if unix/milisegundos > 1: # Está en milisegundos
            unix = unix/1000

        tiempo = datetime(1970,1,1)+timedelta(seconds=unix)
        ajustado =  tiempo - timedelta(hours=horario)
        listo = "_"+ajustado.strftime('%Y-%m-%d %H:%M').replace(" ","_")
        return listo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Programa principal

# COMMAND ----------

sabe=Sabe(container,account,simple_path,url,agent,dbks_scope,service)
folder=dbutils.fs.ls("dbfs:/user/hive/warehouse")
files_star=["accounts_cols/","customers_cols/","payments_cols/"]

for file in folder:
    if file.name in files_star:
        name=file.name.replace("_cols/","")
        file_update=sabe.subida_datos(name,file.modificationTime)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirmación de archivos

# COMMAND ----------

complete_path = f"abfss://{container}@{account}.dfs.core.windows.net/{simple_path}/specs/"
folder = dbutils.fs.ls(complete_path)

for file in folder:
    print(file.path)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminación de archivos

# COMMAND ----------

# blob_name = 'ops/fraud-prevention/specs/customers_2024-08-12_13:02.feather'
# sabe.eliminacion_datos(blob_name)

# COMMAND ----------

ruta = "abfss://gold@stlakehyliaqas.dfs.core.windows.net/ops/fraud-prevention/reports/customers/"
ruta = "abfss://gold@stlakehyliaqas.dfs.core.windows.net/ops/fraud-prevention/specs/payments_specs_latest.feather"
folder = dbutils.fs.ls(ruta)
print(folder)
# for archivo in folder:
#     print(archivo.name,archivo.path)

import re 
cadena = "CONCILIAFZE0220230104"

# Expresión regular para encontrar números
numeros = re.findall(r'\d+', cadena)

print(numeros)

# file = spark.read.format("csv").option("delimiter", "|").load(ruta+"/CONCILIAFZE0220230104.txt")

# file.display()
