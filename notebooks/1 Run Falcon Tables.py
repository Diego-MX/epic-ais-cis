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

# MAGIC %pip install -q git+https://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@github.com/Bineo2/data-python-tools.git@dev-diego

# COMMAND ----------

### Celda para algunos ajustes en desarrollo. 

from pathlib import Path
ref_path = Path("../refs/upload-specs")

import re
from pyspark.sql import types as T, functions as F, Row, Column
from toolz.dicttoolz import valmap

from epic_py.delta import column_name
from config import falcon_handler


# COMMAND ----------

from copy import deepcopy
from datetime import datetime as dt, date
import pandas as pd
from pytz import timezone

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

from importlib import reload
import epic_py; reload(epic_py)
import config ; reload(config)

from epic_py.delta import EpicDF, EpicDataBuilder
from epic_py.platform import EpicIdentity
from config import (falcon_handler, falcon_rename, 
    dbks_tables, blob_path)


falcon_builder = EpicDataBuilder(typehandler=falcon_handler)

def check_builder(build_dict): 
    check_keys = list(x for x in build_dict.keys() if x not in ['_val', 'None'])
    if len(check_keys) == 0: 
        print("Builder is empty.")
    elif len(check_keys) == 1: 
        print(f"Builder can be computed.\nUse {check_keys}.")
    else: 
        print(f"Builder needs JOIN(s).\n{check_keys}.")
    return

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

customers_1 = (EpicDF(spark, dbks_tables['gld_client_file'])
    .select_plus(customers_extract['gld_client_file'])
    .with_column_plus(customers_extract['_val'])
    .with_column_plus(customers_extract['None']))

customers_2 = (customers_1.select_plus(customers_loader))
customers_2.display()



cust_header = dict(option=True, vendor='fiserv', 
    data={
        'record_type'       : 'B', 
        'filetype'          : 'CIS20', 
        'system_id'         : 'PMAX', 
        'data_feed_sort_key': 0, 
        'layout_ver'        : 1.0, 
        'filler'            : 1866, 
        'append'            : -1})

cust_trailer = deepcopy(cust_header)
cust_trailer['data'].update({
        'record_type'       : 'E', 
        'data_feed_sort_key': 10**9-1, 
        'append'            : 1})

customers_3 = (customers_2
    .select(customers_onecol)
    .prep_one_col(header_info=cust_header, trailer_info=cust_trailer))

customers_3.save_as_file(
    f"{blob_path}/reports/customers/{cust_time}.csv", 
    f"{blob_path}/reports/customers/tmp_delta", 
    header=False)


# COMMAND ----------

customers_3.display()

# COMMAND ----------

def check_1(rr): 
    check_col = (F.when(F.length(rr['name']) != F.lit(rr['len']), 
        F.concat(F.lit(f"{rr['len']}:{{"), F.col(rr['name']), F.lit("}:"), F.length(rr['name'])))
        .otherwise("ok"))
    return check_col

check_select = {rr['name']: check_1(rr) for _, rr in customers_specs.iterrows()}

df_check = (customers_2
    .select_plus(check_select))

count_cols = (df_check
    .withColumn('group', F.lit(1))
    .groupBy('group')
    .agg_plus({rr['name']: F.sum(F.when(F.col(rr['name']) == F.lit('ok'), 0).otherwise(1)) 
        for _, rr in customers_specs.iterrows()}))

check_cols = [a_col for a_col in df_check.columns if count_cols.first()[a_col] > 0]

df_check.select(*check_cols).display()

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

accounts_1.display()

# COMMAND ----------

accounts_2 = accounts_1.select_plus(accounts_loader)

accounts_2.display()

acct_header = dict(option=True, vendor='fiserv', 
    data={
        'record_type'       : 'B', 
        'system_id'         : 'PMAX',
        'filetype'          : 'AIS20', 
        'data_feed_sort_key': 0, 
        'layout_ver'        : 1.0, 
        'filler'            : 1136, 
        'append'            : -1})
acct_trailer = deepcopy(acct_header)
acct_trailer['data'].update({
        'record_type'       : 'E', 
        'data_feed_sort_key': 10**9-1, 
        'append'            : 1})

accounts_3 = (accounts_2
    .select(accounts_onecol)
    .prep_one_col(header_info=acct_header, trailer_info=acct_trailer))

accounts_3.save_as_file(
    f"{blob_path}/reports/accounts/{acct_time}.csv", 
    f"{blob_path}/reports/accounts/tmp_delta", 
    header=False)

# COMMAND ----------

accounts_3.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Pagos

# COMMAND ----------

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
