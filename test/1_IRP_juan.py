# Databricks notebook source
# MAGIC %md
# MAGIC # IRP_Juan
# MAGIC
# MAGIC Este _notebook_ funciona como el espacio para poder hacer **investigaciones**, **reparaciones** y **pruebas** por eso se llama IRP

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Trabajando andamos ...
# MAGIC El header de AIS cambia de lugar cuando se escribe el archivo
# MAGIC

# COMMAND ----------

import dbks_dependencies as deps # pylint: disable=import-error

deps.gh_epicpy('meetme-1',
    tokenfile='../user_databricks.json', typing=False, verbose=True)

# COMMAND ----------

# pylint: disable=consider-using-f-string
# pylint: disable=expression-not-assigned
# pylint: disable=invalid-name
# pylint: disable=import-error
# pylint: disable=no-name-in-module
# pylint: disable=wrong-import-order
# pylint: disable=non-ascii-module-import
# pylint: disable=unused-import
# pylint: disable=wrong-import-position
# pylint: disable=unnecessary-lambda-assignment
# pylint: disable=trailing-whitespace
# pylint: disable=pointless-statement

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

row_name = lambda row: "{name}-{len}".format(**row)

def replace_if(eq_val, rep_val):
    """Remplaza el valor de acuerdo a la situación"""
    # xx -> rep_val if xx == eq_val else xx
    # xx -> if_else(rep_val, equal_to(eq_val)(xx), xx)
    # xx -> if_else(constant(rep_val)(xx), equal_to(eq_val)(xx), identity(xx))
    # xx -> if_else(*juxt(constant(rep_val), equal_to(eq_val), identity)(xx))
    # compose(packed(if_else), juxt(constant(rep_val), equal_to(eq_val), identity))
    # Más complicado 😒
    return (lambda xx: rep_val if xx == eq_val else xx)

def get_time(a_tz="America/Mexico_City", time_fmt="%Y-%m-%d"):
    """Obtención del tiempo"""
    return dt.now(tz=tz(a_tz)).strftime(format=time_fmt)

date_str = lambda ss: dt.strptime(ss, '%Y-%m-%d').date()

dates_by_env = {'qas': '2022-01-01', 'prd': '2023-05-01', None: '2023-01-01'}
default_path = "../refs/upload-specs"


# COMMAND ----------

haz_pagos = 'true'

specs_local =  'true'
at_specs = default_path if specs_local else f"{app_path}/specs"
gold_container = app_resourcer.get_storage_client(None, 'gold')

w_stub = 'true'

falcon_builder = EpicDataBuilder(typehandler=falcon_handler)

datalake = app_resourcer['storage']
dlk_permissions = app_agent.prep_dbks_permissions(datalake, 'gen2')
app_resourcer.set_dbks_permissions(dlk_permissions)


# COMMAND ----------

es_ligera = F.col('ProductID').isin(['EPC_TA_N2', 'EPC_TA_MA1'])

accounts_transform = (lambda accs_df: accs_df
    .withColumnRenamed('ID', 'BorrowerID')
    .filter(es_ligera)
    .withColumn('Type', F.when(es_ligera, 'D').otherwise('ProductID')))


# COMMAND ----------

dbks_tables['accounts']

# COMMAND ----------

acct_time = get_time()

if specs_local:
    accounts_specs = (pd.read_feather(f"{at_specs}/accounts_cols.feather")
        .rename(columns=falcon_rename))
else:
    b_blob = gold_container.get_blob_client(f"{at_specs}/accounts_specs_latest.feather")
    b_data = b_blob.download_blob()
    b_strm = BytesIO()
    b_data.readinto(b_strm)
    b_strm.seek(0)
    accounts_specs = (pd.read_feather(b_strm)
        .rename(columns=falcon_rename))

accounts_specs.loc[1, 'column'] = 'modelSTUB' if w_stub else 'RBTRAN'

ais_longname = '~'.join(row_name(rr)
        for _, rr in accounts_specs.iterrows())
ais_name = ais_longname if COL_DEBUG else 'ais-columna-fixed-width'

accounts_extract = falcon_builder.get_extract(accounts_specs, 'delta')
# Qué columna viene de qué tabla, o qué valor.

accounts_loader = falcon_builder.get_loader(accounts_specs, 'fixed-width')
# Convertir a ancho fijo de acuerdo al tipo de columna.

accounts_onecol = (F.concat(*accounts_specs['name'].values)
    .alias(ais_name))

accounts_0 = accounts_transform(EpicDF(spark, dbks_tables['accounts']))

accounts_1 = (accounts_0
    .select_plus(accounts_extract['accounts'])
    .with_column_plus(accounts_extract['_val'])
    .with_column_plus(accounts_extract['None']))

accounts_1.cache()
accounts_1.count()

accounts_2 = accounts_1.select_plus(accounts_loader)

accounts_3 = (accounts_2
    .select(accounts_onecol)
    .prep_one_col(header_info=headfooters[('account', 'header')],
                 trailer_info=headfooters[('account', 'footer')]))



# COMMAND ----------

# MAGIC %md
# MAGIC # Empaquetado

# COMMAND ----------

def repair_dbks(df_last: DataFrame, df_ancestor:DataFrame, configuration: dict)-> DataFrame:
    """ """
    df_accounts = df_ancestor.select(configuration).toPandas()

    header = df_last.head(1)[0] 
    footer = df_last.tail(1)[0]

    df_header = spark.createDataFrame([header], df_last.columns).toPandas()
    df_footer = spark.createDataFrame([footer], df_last.columns).toPandas()

    df_combinate = pd.concat([df_header,df_accounts], ignore_index = True)
    df_finally = pd.concat([df_combinate,df_footer], ignore_index = True)
    df_finally = spark.createDataFrame(df_finally)

    return EpicDF(df_finally)

# COMMAND ----------

accounts_4 = repair_dbks(accounts_3,accounts_2,accounts_onecol)
accounts_4.display()

# COMMAND ----------

accounts_4.save_as_file(
    f"{app_abfss}/reports/accounts/{acct_time}.csv",
    f"{app_abfss}/reports/accounts/tmp_delta",
    header=False, ignoreTrailingWhiteSpace=False, ignoreLeadingWhiteSpace=False)

# COMMAND ----------

print("Filas AIS-post escritura")
post_ais = (spark.read.format('csv')
    .load(f"{app_abfss}/reports/accounts/{acct_time}.csv"))

ais_inf = (post_ais
    .select(F.length('_c0').alias('ais_longitud'))
    .groupBy('ais_longitud')
    .count())

ais_inf.display()
print("Primera fila AIS-post",post_ais.first())
print("Última fila AIS-post",post_ais.tail(1)[0])

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Trabajando andamos ...
# MAGIC Verificación del tiempo de ejecución de CIS 
# MAGIC

# COMMAND ----------

agg_one = lambda cc: F.any_value(cc).alias(cc)

def one_customers(df_0):
    """Se retiran los usuarios repetidos por los beneficiarios,
    :param df_0: DataFrame con los datos de interes"""
    first_cols = pipe(df_0.columns,
        partial2(remove, ϱ('startswith', ('client_id', 'ben_', 'kyc_')), ...),
        map_z(agg_one))
    df_1 = df_0.groupBy('client_id').agg(*first_cols)
    return df_1

def x_customers(df_0):
    """Se extraen los datos requeridos de los renglones objetivo y los coloca en 
     columnas para poder acceder facilmente.
     :param df_0: DataFrame con los datos de interes"""
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

# PARCHES LOCOS, no borrar hasta que se tenga que borrar
def parche_tablas(df_data):
    """Fue modificada la tabla dim_client separando las columnas kyc y acomodandolas en una
    nueva tabla, la función une de nuevo ambas tablas en una sola para evitar modificar el 
    código
    :param df_data: Es el Dataframe que contiene los datos en cuestión"""
    df_kyc = spark.read.table(f"{ENV}.star_schema.dim_client_kyc")
    df_kyc_select = df_kyc.select("client_id", "kyc_id", "kyc_answer")
    df_return = df_data.join(df_kyc_select, "client_id", how="left")
    return df_return

def clean_caracter(df_data):
    """Se retiran todos las comas y las comillas de los datos provenientes de dim_client
    :param df_data: Es el Dataframe que contiene los datos en cuestión
    """
    df_clean = df_data.select([regexp_replace(col(column), '"',"")
                               .alias(column) for column in df_data.columns])
    df_return = df_clean.select([regexp_replace(col(column), ",", "")
                                 .alias(column) for column in df_clean.columns])
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

customers_0 = EpicDF(spark, dbks_tables['clients'])
customers_0 = clean_caracter(customers_0)


# COMMAND ----------

if ENV == "qas":
    customers_0 = parche_tablas(customers_0) # SE BLOQUEA PORQUE NO SE UTILIZA EN PRD SOLO EN QAS

customers_1 = (one_customers(customers_0)
    .join(x_customers(customers_0), on='client_id')
    .with_column_plus(customers_extract['clients'])
    #.with_column_plus(customers_extract['clients_x']) # no existe en blob
    .with_column_plus(customers_extract['_val'])
    .with_column_plus(customers_extract['None'])
    .join(gender_df_2, on='gender').drop('gender')
    .withColumnRenamed('gender_new', 'gender'))

customers_1.cache()
customers_1.count()


# COMMAND ----------

customers_2 = (customers_1
    .select_plus(customers_loader))

# COMMAND ----------

customers_3 = (customers_2
    .select(customers_onecol)
    .prep_one_col(header_info=headfooters[('customer', 'header')],
                 trailer_info=headfooters[('customer', 'footer')]))

print(customers_0.count(),customers_1.count(),
      customers_2.count(),customers_3.count())
customers_3.display()

# COMMAND ----------


customers_3.save_as_file(
    f"{app_abfss}/reports/customers/{cust_time}.csv",
    f"{app_abfss}/reports/customers/tmp_delta",
    header=False, ignoreTrailingWhiteSpace=False, ignoreLeadingWhiteSpace=False)

# COMMAND ----------

print("Filas CIS-post escritura")
post_cis = (spark.read.format('csv')
    .load(f"{app_abfss}/reports/customers/{cust_time}.csv"))
cis_inf = (post_cis
    .select(F.length('_c0').alias('cis_longitud'))
    .groupBy('cis_longitud')
    .count())

cis_inf.display()
print("Primera fila AIS-post",post_cis.first())
print("Última fila AIS-post",post_cis.tail(1)[0])
