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

### Celda para algunos ajustes en desarrollo. 

# Recargar los módulos que modificamos manualmente. 
# Es equivalente a:
# La intensión es guardar los archivos en el datalake. 
from pathlib import Path
ref_path = Path("../refs/upload-specs")

import re
from pyspark.sql import types as T, functions as F, Row, Column
from toolz.dicttoolz import valmap

from epic_py.delta import column_name
from config import falcon_handler


# COMMAND ----------

from datetime import datetime as dt, date
import pandas as pd
from pytz import timezone

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

name_onecol = '~'.join(f"{nm}-{ln}" for nm, ln in zip(
    customers_specs['name'], customers_specs['len'].astype('int')))

customers_extract = falcon_builder.get_extract(customers_specs, 'delta')
customers_loader = falcon_builder.get_loader(customers_specs, 'fixed-width')
customers_onecol = (F.concat(*customers_specs['name'].values)
    .alias(name_onecol))

customers_0 = EpicDF(spark.read.table(dbks_tables['gld_client_file']))
customers_1 = (customers_0
    .select([vv.alias(kk) 
        for kk, vv in customers_extract['gld_client_file'].items()])
    .with_column_plus(customers_extract['_val'])
    .with_column_plus(customers_extract['None']))

customers_2 = (customers_1.select(customers_loader))

customers_2.display()

customers_3 = customers_2.select(customers_onecol)

customers_3.save_as_file(
    f"{blob_path}/reports/customers/tmp_delta",
    f"{blob_path}/reports/customers/{cust_time}.csv", )


# COMMAND ----------

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

accounts_0 = spark.table(dbks_tables['gld_cx_collections_loans'])

accounts_1 = (EpicDF(accounts_0)
    .select([vv.alias(kk) 
        for kk, vv in accounts_extract['gld_cx_collections_loans'].items()])
    .with_column_plus(accounts_extract['_val'])
    .with_column_plus(accounts_extract['None']))

accounts_2 = accounts_1.select(accounts_loader)

accounts_2.display()
accounts_3 = accounts_2.select(accounts_onecol)

accounts_3.save_as_file(f"{blob_path}/reports/accounts/tmp_delta", 
    f"{blob_path}/reports/accounts/{acct_time}.csv")


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
