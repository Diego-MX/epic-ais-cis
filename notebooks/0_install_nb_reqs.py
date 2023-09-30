# Databricks notebook source
# MAGIC %md
# MAGIC ## Descripción
# MAGIC Instalar librerías generalizadas para los notebooks. 

# COMMAND ----------

# MAGIC %pip install -q pyyaml

# COMMAND ----------

epicpy_ref = 'gh-1.3'   # 'gh-1.2'

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils     # pylint: disable=no-name-in-module,import-error
import subprocess
import yaml

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

with open("../user_databricks.yml", 'r') as _f:     # pylint: disable=unspecified-encoding
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git',
    'ref'   : epicpy_ref,
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token']) }

url_call = "git+https://{token}@{url}@{ref}".format(**epicpy_load)
subprocess.check_call(['pip', 'install', url_call])

# COMMAND ----------

import epic_py
print(f"""
EpicPy version\t: {epic_py.__version__}
Import ref\t: {epicpy_ref}"""[1:])
