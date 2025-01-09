# Databricks notebook source
# MAGIC %md
# MAGIC # IRP_Juan
# MAGIC
# MAGIC Este _notebook_ funciona como el espacio para poder hacer mis **investigaciones**, **reparaciones** y **pruebas** por eso se llama IRP

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Trabajando andamos ...
# MAGIC Epicpy dado que la versión de spark que contiene el pyspark es obsoleto cuando se ingresa un pyspark actual se rompe y causa situaciónes no deseables.
# MAGIC

# COMMAND ----------

import dbks_dependencies as deps

deps.gh_epicpy('meetme-1',  
    tokenfile='../user_databricks.json', typing=False, verbose=True)

# COMMAND ----------

import math 
from pyspark.sql import DataFrame
def get_size(df_dataframe:DataFrame) -> str:
    """:param int bytes: Número entero que contiene el peso del archivo en bytes
    Convertidor de bytes para poder leer con mayor facilidad el dato"""

    bytes = sc._jvm.org.apache.spark.util.SizeEstimator.estimate(df_dataframe._jdf)

    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    type_size = int(math.floor(math.log(bytes, 1024)))
    value_unit = math.pow(1024,type_size)
    size_bytes = round(bytes/value_unit,2)

    return f"{size_bytes} {size_name[type_size]}"

# COMMAND ----------

from datetime import datetime as dt
from io import BytesIO
from operator import methodcaller as ϱ
from pytz import timezone as tz

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F, Row, SparkSession,DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.dbutils import DBUtils     
from toolz import pipe, remove
from toolz.curried import map as map_z

from epic_py.delta import EpicDF, EpicDataBuilder, TypeHandler
from epic_py.tools import dirfiles_df, partial2

from src import (app_agent, app_resourcer, app_abfss, app_path,
    dbks_tables, falcon_types, falcon_rename)
from src.head_foot import headfooters   

from config import ENV # PARCHE MOMENTANEO DADO QUE LAS TABLAS SE MUEVEN >:l

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
falcon_handler = TypeHandler(falcon_types)

# COMMAND ----------

COL_DEBUG = False

w_get = dbutils.widgets.get

row_name = lambda row: "{name}-{len}".format(**row)   

def replace_if(eq_val, rep_val): 
    # xx -> rep_val if xx == eq_val else xx 
    # xx -> if_else(rep_val, equal_to(eq_val)(xx), xx)
    # xx -> if_else(constant(rep_val)(xx), equal_to(eq_val)(xx), identity(xx))
    # xx -> if_else(*juxt(constant(rep_val), equal_to(eq_val), identity)(xx))
    # compose(packed(if_else), juxt(constant(rep_val), equal_to(eq_val), identity))
    # Más complicado 😒
    return (lambda xx: rep_val if xx == eq_val else xx)

def get_time(a_tz="America/Mexico_City", time_fmt="%Y-%m-%d"):
    return dt.now(tz=tz(a_tz)).strftime(format=time_fmt)

date_str = lambda ss: dt.strptime(ss, '%Y-%m-%d').date()

dates_by_env = {'qas': '2022-01-01', 'prd': '2023-05-01', None: '2023-01-01'}
default_path = "../refs/upload-specs"


# COMMAND ----------

# haz_pagos = (w_get('con_pagos').lower() == 'true')

specs_local = True #(w_get('specs_local') == 'true')
at_specs = default_path if specs_local else f"{app_path}/specs"
gold_container = app_resourcer.get_storage_client(None, 'gold')

w_stub = True #(w_get('workflow_stub').lower() == 'true')

falcon_builder = EpicDataBuilder(typehandler=falcon_handler)

datalake = app_resourcer['storage']
dlk_permissions = app_agent.prep_dbks_permissions(datalake, 'gen2')
app_resourcer.set_dbks_permissions(dlk_permissions)


# COMMAND ----------

agg_one = lambda cc: F.any_value(cc).alias(cc)

def one_customers(df_0): 
    first_cols = pipe(df_0.columns, 
        partial2(remove, ϱ('startswith', ('client_id', 'ben_', 'kyc_')), ...), 
        map_z(agg_one))
    df_1 = df_0.groupBy('client_id').agg(*first_cols)
    return df_1

def x_customers(df_0): 
    kyc_cols = {'OCCUPATION': 'x_occupation', 
            'SOURCEOFINCOME': 'x_src_income'}
    kyc_df = (df_0
       .select('client_id', 'kyc_id', 'kyc_answer')
       .groupBy('client_id')
       .pivot('kyc_id', list(kyc_cols.keys()))
       .agg(F.first('kyc_answer'))
       .withColumnsRenamed(kyc_cols))
    df_1 = (df_0
        .withColumn('x_address', F.concat_ws(" ", "addr_street", "addr_external_number"))
        .select('client_id', 'x_address')
        .groupBy('client_id').agg(agg_one('x_address'))
        .join(kyc_df, 'client_id', how='left'))
    return df_1

# COMMAND ----------

# PARCHES LOCOS
def parche_tablas(df_data):
    df_kyc = spark.read.table(f"{ENV}.star_schema.dim_client_kyc")
    df_kyc_select = df_kyc.select("client_id", "kyc_id", "kyc_answer")
    df_return = df_data.join(df_kyc_select, "client_id", how="left")
    return df_return

def clean_caracter(df_data):
    df_clean = df_data.select([regexp_replace(col(column), '"',"").alias(column) for column in df_data.columns])
    df_return = df_clean.select([regexp_replace(col(column), ",", "").alias(column) for column in df_clean.columns]) 
    return df_return

# COMMAND ----------

cust_time = get_time()

if specs_local:
    customers_specs = (pd.read_feather(f"{at_specs}/customers_cols.feather")
        .rename(columns=falcon_rename))

else:
    b_blob = gold_container.get_blob_client(f"{at_specs}/customers_specs_latest.feather")
    b_data = b_blob.download_blob()
    b_strm = BytesIO()
    b_data.readinto(b_strm)
    b_strm.seek(0)
    customers_specs = (pd.read_feather(b_strm)
        .rename(columns=falcon_rename))

customers_specs.loc[1, 'column'] = 'modelSTUB' if w_stub else 'RBTRAN'
cis_longname = '~'.join(row_name(rr) for _, rr in customers_specs.iterrows())
cis_name = cis_longname if COL_DEBUG else 'cis-columna-fixed-width'

name_onecol = '~'.join(row_name(rr)       # pylint: disable=invalid-name
    for _, rr in customers_specs.iterrows())

gender_df_2 = spark.createDataFrame([
    Row(gender='H', gender_new='M'), 
    Row(gender='M', gender_new='F')])

customers_extract = falcon_builder.get_extract(customers_specs, 'delta')
customers_loader  = falcon_builder.get_loader(customers_specs, 'fixed-width')
customers_onecol  = (F.concat(*customers_specs['name'].values)
    .alias(cis_name))

# customers_0.count()

# COMMAND ----------

customers_0 = EpicDF(spark, dbks_tables['clients'])
# customers_0 = clean_caracter(customers_0)
customers_0.cache()
print(get_size(customers_0))

# COMMAND ----------

customers_1 = (one_customers(customers_0)
    .join(x_customers(customers_0), on='client_id')
    .with_column_plus(customers_extract['clients'])
    #.with_column_plus(customers_extract['clients_x']) # no existe en blob
    .with_column_plus(customers_extract['_val'])
    .with_column_plus(customers_extract['None'])
    .join(gender_df_2, on='gender').drop('gender')
    .withColumnRenamed('gender_new', 'gender'))

customers_1.cache()
print(get_size(customers_1))

# COMMAND ----------

customers_1.count()

# COMMAND ----------

from typing import Union
from pyspark.sql import functions as F, types as T, Column, SparkSession

def as_column_p(a_col: Union[Column, str]) -> Column:
    """Helps to working with dataframes when columns aren't the best handled."""
    if   isinstance(a_col, Column):
        return a_col
    elif isinstance(a_col, str):
        return F.col(a_col)
    else:
        print(f"Check type: {type(a_col)}")
        return a_col

def item_col(an_item):
    aa, cc = an_item    # pylint: disable=invalid-name
    return as_column_p(cc).alias(aa)

def select_plus_p(self, cols=Union[dict, list]):
    """{alias: column}"""
    if isinstance(cols, dict):
        cols = map(item_col, cols.items())
    print(self.select(*cols))
    return self.select(*cols)

customers_2_p = select_plus_p(customers_1,customers_loader)

# COMMAND ----------

customers_2 = customers_1.select_plus(customers_loader)

# COMMAND ----------

customers_2.count()

# COMMAND ----------

# 2.11 hrs 
customers_3 = (customers_2
    .select(customers_onecol)
    .prep_one_col(header_info=headfooters[('customer', 'header')],
                 trailer_info=headfooters[('customer', 'footer')]))

# .prep_one_col(header_info=headfooters[('customer', 'header')],
#                  trailer_info=headfooters[('customer', 'footer')])

# COMMAND ----------

customers_3.count()

# COMMAND ----------

print(customers_0.count(),customers_1.count(),
      customers_2.count(),customers_3.count())
customers_3.display()

# customers_3.save_as_file(
#     f"/Workspace/Repos/juan.v@bineo.com/data-ops-fraud-prevention/test/zip_files/{cust_time}.csv",
#     header=False, ignoreTrailingWhiteSpace=False, ignoreLeadingWhiteSpace=False)

# COMMAND ----------


import pandas as pd

a = pd.read_feather("/Workspace/Repos/juan.v@bineo.com/data-ops-fraud-prevention/refs/upload-specs/accounts_cols.feather")

# display(a)

a = pd.read_feather("/Workspace/Repos/juan.v@bineo.com/data-ops-fraud-prevention/refs/upload-specs/customers_cols.feather")

# display(a)

from pyspark.sql import functions as F

b = spark.createDataFrame(a)
count = b.agg(F.count("*")).collect()[0][0]

count
