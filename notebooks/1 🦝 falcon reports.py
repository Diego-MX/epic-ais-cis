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

import dependencies as deps
deps.gh_epicpy('meetme-1', 
    tokenfile='../user_databricks.yml', typing=False, verbose=True)

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

COL_DEBUG = False

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
dbutils.widgets.text('specs_local', 'true', "Archivo Feather p. Specs en Repo")


# COMMAND ----------

haz_pagos = pipe(w_get('con_pagos'), 
    ϱ('lower'), equal_to('true'))

ref_path = pipe(w_get('specs_local'), 
    replace_if('true', default_path), 
    Path)

w_stub = pipe(w_get('workflow_stub'), 
    ϱ('lower'), equal_to('true'))

falcon_builder = EpicDataBuilder(typehandler=falcon_handler)

datalake = app_resourcer['storage']
dlk_permissions = app_agent.prep_dbks_permissions(datalake, 'gen2')
app_resourcer.set_dbks_permissions(dlk_permissions)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuentas

# COMMAND ----------

# MAGIC %md
# MAGIC Se tienen que transformar los tipos de cuenta de acuerdo a los siguientes esquema: 
# MAGIC
# MAGIC ## Para Fiserv
# MAGIC Los siguientes tipos de cuenta se definen en las especificaciones de Excel. 
# MAGIC
# MAGIC | Clave | Descripción                  |
# MAGIC |-------|------------------------------|
# MAGIC | D     | DDA/current account          
# MAGIC | M   | Mortgage
# MAGIC | H   | Home Equity Line of Credit (HELOC)
# MAGIC | S   | Savings
# MAGIC | C   | Credit card
# MAGIC | LU  | Unsecured Loan
# MAGIC | LA |  Automobile Loan
# MAGIC | LS | Secured Loan (other than auto/mortgage)
# MAGIC | UC | Unsecured Line of Credit
# MAGIC | SC | Secured Line of Credit (other than HELOC)
# MAGIC | MM | Money market
# MAGIC | T  | Treasury
# MAGIC | Z  | Certificate of deposit
# MAGIC | B  | Brokerage
# MAGIC | O  | Other Deposit Accounts (Annuity, Life Insurance, and so on)
# MAGIC
# MAGIC ## Accounts Tiene  
# MAGIC Los siguientes tipos de productos se obtienen de la tabla de `current_account` como sigue: 
# MAGIC ````
# MAGIC the_products = (spark.read.table('current_account')
# MAGIC     .)
# MAGIC ```
# MAGIC
# MAGIC |ProductID  |	ProductDescription     | Extraer | Mapeo Fiserv
# MAGIC |-----------|------------------------|---------|-------------
# MAGIC |EPC_TA_N2	|Cuenta bineo ligera (N2)|  ✔️     | D
# MAGIC |EPC_TA_N2	|Cuenta bineo ligera     | ✔️      | D
# MAGIC |EPC_TA_MA1	|Cuenta de ahorro bineo  | ✔️      | D ... ¿S?
# MAGIC |EPC_TA_MA1	|Cuenta bineo total      | ✔️      | D
# MAGIC |EPC_OP_MAX	|EPIC Operaciones de negocios | ❌ |
# MAGIC |EPC_OP_MAX |                      	 | ❌      | 
# MAGIC |EPC_SP_MAX	|                        | ❌      |
# MAGIC |EPC_SP_MAX	|EPIC CPD Cuenta transitoria | ❌  | 
# MAGIC |POCKET1	  |Cuenta Pocket           |  ❌     |
# MAGIC  

# COMMAND ----------

fiserv_transform = (lambda accs_df: accs_df
    .withColumnRenamed('ID', 'BorrowerID')
    .filter(F.col('ProductID').isin(['EPC_TA_N2', 'EPC_TA_MA1']))
    .withColumn('Type', 
        F.when(F.col('ProductID').isin(['EPC_TA_N2', 'EPC_TA_MA1']), 'D')
            .otherwise(F.col('ProductID'))))

# COMMAND ----------

acct_time = get_time()
accounts_specs = (pd.read_feather(ref_path/'accounts_cols.feather')
        .rename(columns=falcon_rename))
accounts_specs.loc[1, 'column'] = 'modelSTUB' if w_stub else 'RBTRAN'

ais_longname = '~'.join(λ_name(rr) 
        for _, rr in accounts_specs.iterrows())
ais_name = ais_longname if COL_DEBUG else 'ais-columna-fixed-width'

accounts_extract = falcon_builder.get_extract(accounts_specs, 'delta')
# Qué columna viene de qué tabla, o qué valor. 

accounts_loader = falcon_builder.get_loader(accounts_specs, 'fixed-width')
# Convertir a ancho fijo de acuerdo al tipo de columna. 

accounts_onecol = (F.concat(*accounts_specs['name'].values)
    .alias(ais_name))

accounts_tbl = app_resourcer.get_resource_url('abfss', 'storage',
    container='bronze', blob_path=dbks_tables['accounts'])  # Tiene que ser Current Account. 

accounts_0 = fiserv_transform(EpicDF(spark, accounts_tbl))

accounts_1 = (accounts_0
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

# MAGIC %md
# MAGIC ## Clientes
# MAGIC
# MAGIC La columna original para tomar la información del cliente era `gld_client_file`.  
# MAGIC Esta fue eliminada sin aviso y por eso empezó a fallar todo.  
# MAGIC La tabla equivalente es `star_schema.dim_client`.  
# MAGIC Aunque se supone que tiene más estructura que la anterior, la realidad es que no está bien hecha.  
# MAGIC
# MAGIC El _hack_ se compone de lo siguiente:  
# MAGIC * Mapeo de columnas `prep_columns`.  
# MAGIC * Un increíble _pivoteo_ de columnas de `kyc`.  
# MAGIC * Un filtrado de datos repetidos debido al desmadre que se hizo con `kyc`.  

# COMMAND ----------


mod_columns = {
    "user_first_name": F.col("first_name"),
    "user_first_last_name": F.col("last_name"), 
    "user_second_last_name": F.col("last_name2"), 
    "sap_client_id": F.col("client_id"),
    "user_phone_number": F.col("phone_number"),
    "user_email": F.col("current_email_address"),
    "fad_birth_date": F.col("birth_date"),
    "fad_birth_cntry": F.col("birth_place_name"), 
    "user_neighborhood": F.col("addr_district"), 
    "fad_gender": F.col("person_gender"), 
    "fad_state": F.col("region"), 
    'user_rfc': F.col("person_rfc"), 
    "fad_addr_1": F.concat_ws(" ", "addr_street", "addr_external_number")}

customers_kyc = (EpicDF(spark, "qas.star_schema.dim_client")
    .select('client_id', 'kyc_id', 'kyc_answer')
    .filter(F.col('kyc_id').isin(['OCCUPATION', 'SOURCEOFINCOME']))
    .groupBy('client_id').pivot('kyc_id').agg({'kyc_answer': 'first'})
    .withColumnsRenamed({
        'client_id': 'sap_client_id', 
        'OCCUPATION': 'kyc_occupation', 
        'SOURCEOFINCOME': 'kyc_src_income'}))

customers_fix = (EpicDF(spark, "qas.star_schema.dim_client")
    .select_plus(mod_columns)
    .distinct()
    .join(customers_kyc, 'sap_client_id', how='left'))

customers_fix.display()


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

gender_df = EpicDF(spark, pd.DataFrame(
        columns=['gender', 'gender_new'], 
        data=[['H', 'M'], ['M', 'F']]))

# customers_leg = EpicDF(spark, dbks_tables['gld_client_file'])
# customers_legacy = (customers_leg
#     .drop('bureau_req_ts', *filter(ϱ('startswith', 'ben_'), customers_leg.columns))
#     .distinct())

customers_2 = (EpicDF(customers_fix)
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
