# Databricks notebook source
# MAGIC %md 
# MAGIC # Tablas de Falcon
# MAGIC
# MAGIC En este _notebook_ se ejecutan las actualizaciones de las tablas de Falcon.  
# MAGIC Las secciones del _notebook_ son:  
# MAGIC `-1` Desarrollo  
# MAGIC `0`  Preparación  
# MAGIC `1.1`  Clientes  
# MAGIC `1.2`  Ejecutamos las tablas una por una, para tener visibilidad de errores:  
# MAGIC
# MAGIC * Clientes
# MAGIC * Cuentas
# MAGIC * Pagos

# COMMAND ----------

from copy import deepcopy
from datetime import datetime as dt, date
import pandas as pd
from pyspark.sql import SparkSession, functions as F 
from pyspark.dbutils import DBUtils
from pytz import timezone
import subprocess
import yaml

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

with open("../user_databricks.yml", 'r') as _f: 
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': 'dev-diego', 
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token']) }  

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
subprocess.check_call(['pip', 'install', url_call])

# COMMAND ----------

from pathlib import Path
ref_path = Path("../refs/upload-specs")

# COMMAND ----------

from importlib import reload
import config; reload(config)
import epic_py; reload(epic_py)

from pathlib import Path
from epic_py.delta import EpicDF, EpicDataBuilder
from src.head_foot import headfooters
from config import (falcon_handler, falcon_rename, 
    dbks_tables, blob_path, app_resourcer)

falcon_builder = EpicDataBuilder(typehandler=falcon_handler)

def get_time(tz="America/Mexico_City", time_fmt="%Y-%m-%d"): 
    return dt.now(tz=timezone(tz)).strftime(format=time_fmt)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Clientes
# MAGIC

# COMMAND ----------

cust_time = get_time()
customers_specs = (pd.read_feather(ref_path/'customers_cols.feather')
        .rename(columns=falcon_rename))

name_onecol = '~'.join(f"[{int(rr['len'])}]{rr['name']}" 
    for _, rr in customers_specs.iterrows())

customers_extract = falcon_builder.get_extract(customers_specs, 'delta')
customers_loader  = falcon_builder.get_loader(customers_specs, 'fixed-width')
customers_onecol  = (F.concat(*customers_specs['name'].values)
    .alias(name_onecol))

customers_1= (EpicDF(spark, dbks_tables['gld_client_file'])
    .with_column_plus(customers_extract['gld_client_file'])
    .with_column_plus(customers_extract['_val'])
    .with_column_plus(customers_extract['None']))

customers_2 = (customers_1.select_plus(customers_loader))
customers_2.display()

customers_3 = (customers_2
    .select(customers_onecol)
    .prep_one_col(header_info=headfooters[('customer', 'header')], 
                 trailer_info=headfooters[('customer', 'footer')]))

customers_3.save_as_file(
    f"{blob_path}/reports/customers/{cust_time}.csv", 
    f"{blob_path}/reports/customers/tmp_delta", 
    header=False)


# COMMAND ----------

print(f"{blob_path}/reports/customers/{cust_time}.csv")
customers_3.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Cuentas

# COMMAND ----------

acct_time = get_time()
accounts_specs = (pd.read_feather(ref_path/'accounts_cols.feather')
        .rename(columns=falcon_rename))

onecol_account = '~'.join(f"{nm}-{ln}" 
    for nm, ln in zip(accounts_specs['name'], accounts_specs['len'].astype('int')))

accounts_extract = falcon_builder.get_extract(accounts_specs, 'delta')
accounts_loader = falcon_builder.get_loader(accounts_specs, 'fixed-width')
accounts_onecol = (F.concat(*accounts_specs['name'].values)
    .alias(onecol_account))

accounts_1 = (EpicDF(spark, dbks_tables['gld_cx_collections_loans'])
    .select_plus(accounts_extract['gld_cx_collections_loans'])
    .with_column_plus(accounts_extract['_val'])
    .with_column_plus(accounts_extract['None']))


# COMMAND ----------

accounts_2 = accounts_1.select_plus(accounts_loader)

accounts_2.display()

accounts_3 = (accounts_2
    .select(accounts_onecol)
    .prep_one_col(header_info=headfooters[('account', 'header')], 
                 trailer_info=headfooters[('account', 'footer')]))

accounts_3.save_as_file(
    f"{blob_path}/reports/accounts/{acct_time}.csv", 
    f"{blob_path}/reports/accounts/tmp_delta", 
    header=False)

# COMMAND ----------

print( f"{blob_path}/reports/accounts/{acct_time}.csv")
accounts_3.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Pagos

# COMMAND ----------

haz_pagos = False

if haz_pagos: 
    pymt_time = get_time()
    payments_specs = (pd.read_feather(ref_path/'payments_cols.feather')
            .rename(columns=falcon_rename))

    payments_extract = falcon_builder.get_extract(payments_specs, 'delta')
    payments_loader = falcon_builder.get_loader(payments_specs, 'fixed-width')
    payments_onecol = (F.concat(*payments_specs['name'].values)
        .alias('one-column'))

    payments_0 = spark.table(dbks_tables['gld_cx_collections_loans'])

    payments_1 = (EpicDF(payments_0)
        .select([vv.alias(kk) 
            for kk, vv in payments_extract['gld_cx_collections_loans'].items()])
        .with_column_plus(payments_extract['_val'])
        .with_column_plus(payments_extract['None'])
        )

    payments_2 = payments_1.select(payments_loader)

    payments_3 = payments_2.select(payments_onecol)

    payments_3.display()
