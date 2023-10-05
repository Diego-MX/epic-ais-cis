# Databricks notebook source
# MAGIC %md
# MAGIC # Tablas de Falcon
# MAGIC
# MAGIC En este _notebook_ se ejecutan las actualizaciones de las tablas de Falcon.
# MAGIC Las secciones del _notebook_ son:  
# MAGIC `-1` Desarrollo  
# MAGIC `0`  Preparación  
# MAGIC `1.1` Clientes  
# MAGIC `1.2` Ejecutamos las tablas una por una, para tener visibilidad de errores:  
# MAGIC   
# MAGIC * Clientes  
# MAGIC * Cuentas  
# MAGIC * Pagos

# COMMAND ----------

# MAGIC %run ./0_install_nb_reqs

# COMMAND ----------

# pylint: disable=invalid-name
haz_pagos = False
match_clientes = True
nombre_columna = False
repo_path = True

# COMMAND ----------

# pylint: disable=wrong-import-position
# pylint: disable=wrong-import-order
from datetime import datetime as dt, date
from pathlib import Path
import re
from warnings import warn

import pandas as pd
from pyspark.sql import functions as F, Row, SparkSession, Window as W
from pyspark.dbutils import DBUtils     # pylint: disable=no-name-in-module,import-error
from pytz import timezone as tz

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

if match_clientes: 
    customers_ids = (EpicDF(spark, dbks_tables['gld_client_file'])
        .select('sap_client_id').distinct())
    which_ids = (EpicDF(spark, 'prd.hyrule.view_account_balance_mapper')
        .withColumn('sap_client_id', F.col('client_id'))
        .distinct()
        .join(customers_ids, on='sap_client_id', how='inner'))
    which_ids.display()

if repo_path: 
    ref_path = Path("../refs/upload-specs")
else: 
    warn("Havent implemented REF_PATH for Blob Path (opposite to Repo Path).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuentas

# COMMAND ----------

acct_time = get_time()
accounts_specs = (pd.read_feather(ref_path/'accounts_cols.feather')
        .rename(columns=falcon_rename))

ais_longname = '~'.join(λ_name(rr)        # pylint: disable=invalid-name
    for _, rr in accounts_specs.iterrows())
ais_name = ais_longname if nombre_columna else 'ais-columna-fixed-width'

accounts_extract = falcon_builder.get_extract(accounts_specs, 'delta')
accounts_loader = falcon_builder.get_loader(accounts_specs, 'fixed-width')
accounts_onecol = (F.concat(*accounts_specs['name'].values)
    .alias(ais_name))

accounts_tbl = (dbks_tables['gld_cx_collections_loans'] 
    if not match_clientes else 'prd.hyrule.view_account_balance_mapper')

if match_clientes: 
    accounts_0 = (EpicDF(spark, accounts_tbl)
        .join(which_ids, on='client_id', how='semi')
        .with_column_renamed_plus({
            'client_id': 'BorrowerID', 'cms_account_id': 'BankAccountID'}))
else: 
    accounts_0 = EpicDF(spark, accounts_tbl)

accounts_1 = (accounts_0
    .withColumn('type', F.lit('D'))
    .select_plus(accounts_extract['gld_cx_collections_loans'])
    .with_column_plus(accounts_extract['_val'])
    .with_column_plus(accounts_extract['None']))

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

accounts_loader = falcon_builder.get_loader(accounts_specs, 'fixed-width')

# COMMAND ----------

print(f"accounts/{acct_time}.csv")
accounts_3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clientes
# MAGIC

# COMMAND ----------

cust_time = get_time()
customers_specs = (pd.read_feather(ref_path/'customers_cols.feather')
    .rename(columns=falcon_rename))

cis_longname = '~'.join(λ_name(rr) for _, rr in customers_specs.iterrows())
cis_name = cis_longname if nombre_columna else 'cis-columna-fixed-width'

customers_extract = falcon_builder.get_extract(customers_specs, 'delta')
customers_loader  = falcon_builder.get_loader(customers_specs, 'fixed-width')
customers_onecol  = (F.concat(*customers_specs['name'].values)
    .alias(cis_name))

gender_df = spark.createDataFrame([
    Row(gender='H', gender_new='M'), 
    Row(gender='M', gender_new='F')])

if match_clientes: 
    customers_0 = (EpicDF(spark, dbks_tables['gld_client_file'])
        .join(which_ids, on='sap_client_id', how='semi'))
else: 
    customers_0 = EpicDF(spark, dbks_tables['gld_client_file'])

customers_1= EpicDF(customers_0
    .drop('bureau_req_ts', *(a_col for a_col in customers_0.columns
        if a_col.startswith('ben_')))
    .distinct())

customers_2 = EpicDF(customers_1
    .with_column_plus(customers_extract['gld_client_file'])
    .with_column_plus(customers_extract['_val'])
    .with_column_plus(customers_extract['None'])
    .join(gender_df, on='gender')
    .drop('gender')
    .withColumnRenamed('gender_new', 'gender'))

customers_2.display()

customers_3 = (customers_2
    .select_plus(customers_loader))

customers_4 = (customers_3
    .select(customers_onecol)
    .prep_one_col(header_info=headfooters[('customer', 'header')],
                 trailer_info=headfooters[('customer', 'footer')]))

customers_4.save_as_file(
    f"{blob_path}/reports/customers/{cust_time}.csv",
    f"{blob_path}/reports/customers/tmp_delta",
    header=False)

# COMMAND ----------

print(f"customers/{cust_time}.csv")
customers_4.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pagos

# COMMAND ----------

if haz_pagos:
    pymt_time = get_time()
    payments_specs = (pd.read_feather(ref_path/'payments_cols.feather')
            .rename(columns=falcon_rename))

    one_column = '~'.join(λ_name(rr) for _, rr in payments_specs.iterrows())    # pylint: disable=invalid-name
    payments_extract = falcon_builder.get_extract(payments_specs, 'delta')
    payments_loader = falcon_builder.get_loader(payments_specs, 'fixed-width')
    payments_onecol = (F.concat(*payments_specs['name'].values)
        .alias(one_column))

    payments_0 = spark.table(dbks_tables['gld_cx_collections_loans'])

    payments_1 = (EpicDF(payments_0)
        .select([vv.alias(kk)
            for kk, vv in payments_extract['gld_cx_collections_loans'].items()])
        .with_column_plus(payments_extract['_val'])
        .with_column_plus(payments_extract['None']))

    payments_2 = payments_1.select(payments_loader)

    payments_3 = payments_2.select(payments_onecol)

    payments_3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Resultados

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Longitud de filas

# COMMAND ----------

print("Filas CIS")
(customers_4
    .select(F.length(cis_name).alias('cis_longitud'))
    .groupBy('cis_longitud')
    .count()
    .display())

# COMMAND ----------

print("Filas AIS")
(accounts_3
    .select(F.length(ais_name).alias('ais_longitud'))
    .groupBy('ais_longitud')
    .count()
    .display())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2. Exploración de archivos

# COMMAND ----------

cis_path = f"{blob_path}/reports/customers/"
print(f"""
CIS Path:\t{cis_path}
(horario UTC)"""[1:])
(dirfiles_df(cis_path, spark)
    .loc[:, ['name', 'modificationTime', 'size']])

# COMMAND ----------

ais_path = f"{blob_path}/reports/accounts/"
print(f"""
AIS Path:\t{ais_path}
(horario UTC)"""[1:])
(dirfiles_df(ais_path, spark)
    .loc[:, ['name', 'modificationTime', 'size']])
