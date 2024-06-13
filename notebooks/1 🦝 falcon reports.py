# Databricks notebook source
# MAGIC %md
# MAGIC # Tablas de Falcon
# MAGIC
# MAGIC En este _notebook_ se ejecutan las actualizaciones de las tablas de Falcon.  
# MAGIC Las secciones del _notebook_ son:  
# MAGIC `0` Desarrollo y preparación  
# MAGIC `1` Tablas  
# MAGIC   `1.1`  Cuentas  
# MAGIC   `1.2`  Clientes  
# MAGIC   `1.3`  Pagos  
# MAGIC `2` Resultados: incluye análisis generales para validar la ejecución.   

# COMMAND ----------

from src import dependencies as deps
deps.gh_epicpy('meetme-1', 
    tokenfile='../user_databricks.yml', typing=False, verbose=True)

# COMMAND ----------

import pytest

# COMMAND ----------

# pylint: disable=wrong-import-position
# pylint: disable=wrong-import-order
# pylint: disable=invalid-name
from datetime import datetime as dt
from operator import methodcaller as ϱ, eq
from os import getenv
from pathlib import Path
from pytz import timezone as tz

import pandas as pd
from pyspark.sql import functions as F, Row, SparkSession   # pylint: disable=import-error
from pyspark.dbutils import DBUtils     # pylint: disable=no-name-in-module,import-error
from toolz import curry, pipe

from epic_py.delta import EpicDF, EpicDataBuilder
from epic_py.tools import dirfiles_df

from src.head_foot import headfooters   # pylint: disable=ungrouped-imports
from src import (app_agent, app_resourcer, blob_path,
    dbks_tables, falcon_handler, falcon_rename)

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

w_get = dbutils.widgets.get

λ_name = lambda row: "{name}-{len}".format(**row)   # pylint: disable=consider-using-f-string

equal_to = curry(eq)

def replace_if(eq_val, rep_val): 
    return (lambda xx: rep_val if xx == eq_val else xx)

def get_time(a_tz="America/Mexico_City", time_fmt="%Y-%m-%d"):
    return dt.now(tz=tz(a_tz)).strftime(format=time_fmt)

date_str = lambda ss: dt.strptime(ss, '%Y-%m-%d').date()

dates_by_env = {'qas': '2022-01-01', 'prd': '2023-05-01', None: '2023-01-01'}
default_path = "../refs/upload-specs"

dbutils.widgets.text('con_pagos', 'false', "Ejecutar PIS-Payment Info. Sec.")
dbutils.widgets.text('workflow_stub', 'true', "Nombre de workflow como campo en reportes.")
dbutils.widgets.text('hack_clients', 'false', "Hack para empatar los clientes de CIS y AIS.")
dbutils.widgets.text('specs_local', 'true', "Archivo Feather p. Specs en Repo")


# COMMAND ----------

haz_pagos = pipe(w_get('con_pagos'), 
    ϱ('lower'), equal_to('true'))

ref_path = pipe(w_get('specs_local'), 
    replace_if('true', default_path), 
    Path)

w_stub = pipe(w_get('workflow_stub'), 
    ϱ('lower'), equal_to('true'))

hack_clients = pipe(w_get('hack_clients'),
    ϱ('lower'), equal_to('true'))

falcon_builder = EpicDataBuilder(typehandler=falcon_handler)

datalake = app_resourcer['storage']
dlk_permissions = app_agent.prep_dbks_permissions(datalake, 'gen2')
app_resourcer.set_dbks_permissions(dlk_permissions)


# COMMAND ----------

COL_DEBUG = False

if hack_clients: 
    ids_c = (EpicDF(spark, dbks_tables['gld_client_file'])
        .select('sap_client_id')
        .distinct())
    
    # referencia de 19 dígitos:  F.concat(F.lit('765'), F.col('cms_account_id')), 
    which_ids = (EpicDF(spark, dbks_tables['match_clients'])
        .with_column_plus({
            'BankAccountID': F.substring('core_account_id', 1, 11), 
            'BorrowerID': F.col('client_id')})
        .distinct()
        .join(ids_c, how='inner', 
            on=(ids_c['sap_client_id'] == F.col('BorrowerID'))))
    which_ids.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuentas

# COMMAND ----------

acct_time = get_time()
accounts_specs = (pd.read_feather(ref_path/'accounts_cols.feather')
        .rename(columns=falcon_rename))
accounts_specs.loc[1, 'column'] = 'modelSTUB' if w_stub else 'RBTRAN'

ais_longname = '~'.join(λ_name(rr) 
        for _, rr in accounts_specs.iterrows())
ais_name = ais_longname if COL_DEBUG else 'ais-columna-fixed-width'

accounts_extract = falcon_builder.get_extract(accounts_specs, 'delta')
accounts_loader = falcon_builder.get_loader(accounts_specs, 'fixed-width')
accounts_onecol = (F.concat(*accounts_specs['name'].values)
    .alias(ais_name))

accounts_tbl = (dbks_tables['gld_cx_collections_loans'] 
    if not hack_clients else 'prd.hyrule.view_account_balance_mapper')

if hack_clients: 
    accounts_0 = (EpicDF(spark, accounts_tbl)
        .join(which_ids, how='inner', on='client_id'))
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
    header=False, ignoreTrailingWhiteSpace=False, ignoreLeadingWhiteSpace=False)

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
customers_specs.loc[1, 'column'] = 'modelSTUB' if w_stub else 'RBTRAN'

cis_longname = '~'.join(λ_name(rr) for _, rr in customers_specs.iterrows())
cis_name = cis_longname if COL_DEBUG else 'cis-columna-fixed-width'

name_onecol = '~'.join(λ_name(rr)       # pylint: disable=invalid-name
    for _, rr in customers_specs.iterrows())

customers_extract = falcon_builder.get_extract(customers_specs, 'delta')
customers_loader  = falcon_builder.get_loader(customers_specs, 'fixed-width')
customers_onecol  = (F.concat(*customers_specs['name'].values)
    .alias(cis_name))

gender_df = spark.createDataFrame([
    Row(gender='H', gender_new='M'), 
    Row(gender='M', gender_new='F')])

if hack_clients: 
    customers_0 = (EpicDF(spark, dbks_tables['gld_client_file'])
        .join(which_ids, on='sap_client_id', how='semi'))
else: 
    customers_0 = EpicDF(spark, dbks_tables['gld_client_file'])

customers_1= EpicDF(customers_0
    .drop('bureau_req_ts', *filter(ϱ('startswith', 'ben_'), customers_0.columns))
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
    header=False, ignoreTrailingWhiteSpace=False, ignoreLeadingWhiteSpace=False)

# COMMAND ----------

print(f"customers/{cust_time}.csv")
customers_3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pagos

# COMMAND ----------

if haz_pagos:
    pymt_time = get_time()
    payments_specs = (pd.read_feather(ref_path/'payments_cols.feather')
            .rename(columns=falcon_rename))
    payments_specs.loc[1, 'column'] = 'modelSTUB' if w_stub else 'RBTRAN'

    one_column = '~'.join(map(λ_name, payments_specs.itertuples()))    # pylint: disable=invalid-name

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

print("Filas AIS-post escritura")
post_ais = (spark.read.format('csv')
    .load(f"{blob_path}/reports/accounts/{cust_time}.csv"))
(post_ais
    .select(F.length('_c0').alias('ais_longitud'))
    .groupBy('ais_longitud')
    .count()
    .display())

# COMMAND ----------

print("Filas CIS-post escritura")
post_cis = (spark.read.format('csv')
    .load(f"{blob_path}/reports/customers/{cust_time}.csv"))
(post_cis
    .select(F.length('_c0').alias('cis_longitud'))
    .groupBy('cis_longitud')
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
(dirfiles_df(cis_path, spark)   # pylint: disable=expression-not-assigned
    .loc[:, ['name', 'modificationTime', 'size']])

# COMMAND ----------

ais_path = f"{blob_path}/reports/accounts/"
print(f"""
AIS Path:\t{ais_path}
(horario UTC)"""[1:])
(dirfiles_df(ais_path, spark)   # pylint: disable=expression-not-assigned
    .loc[:, ['name', 'modificationTime', 'size']])
