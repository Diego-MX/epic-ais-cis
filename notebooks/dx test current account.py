# Databricks notebook source
# MAGIC %run ./0_install_nb_reqs

# COMMAND ----------

# pylint: disable=wrong-import-position
# pylint: disable=wrong-import-order
from datetime import datetime as dt, date
from pytz import timezone as tz

import pandas as pd
from pyspark.sql import functions as F, SparkSession, Window as W 
from pyspark.dbutils import DBUtils     # pylint: disable=no-name-in-module,import-error

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

from epic_py.delta import EpicDF, EpicDataBuilder
from epic_py.tools import dirfiles_df
from src.head_foot import headfooters
from config import (app_agent, app_resourcer, blob_path,
    dbks_tables, falcon_handler, falcon_rename)

falcon_builder = EpicDataBuilder(typehandler=falcon_handler)

datalake = app_resourcer['storage']
dlk_permissions = app_agent.prep_dbks_permissions(datalake, 'gen2')
app_resourcer.set_dbks_permissions(dlk_permissions)

λ_name = lambda row: "{name}-{len}".format(**row)   # pylint: disable=consider-using-f-string

def get_time(a_tz="America/Mexico_City", time_fmt="%Y-%m-%d"):
    return dt.now(tz=tz(a_tz)).strftime(format=time_fmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Current Account

# COMMAND ----------

spark.sql("show tables in nayru_accounts").display()
