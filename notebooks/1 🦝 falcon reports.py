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

import dbks_dependencies as deps
deps.gh_epicpy('meetme-1',  
    tokenfile='../user_databricks.json', typing=False, verbose=True)

# COMMAND ----------

# pylint: disable=consider-using-f-string
# pylint: disable=expression-not-assigned
# pylint: disable=invalid-name
# pylint: disable=import-error
# pylint: disable=no-name-in-module
# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position

from datetime import datetime as dt
from io import BytesIO
from operator import methodcaller as ϱ
from pytz import timezone as tz

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F, Row, SparkSession
from pyspark.dbutils import DBUtils     
from toolz import pipe, remove
from toolz.curried import map as map_z

from epic_py.delta import EpicDF, EpicDataBuilder, TypeHandler
from epic_py.tools import dirfiles_df, partial2


from src import (app_agent, app_resourcer, app_abfss, app_path,
    dbks_tables, falcon_types, falcon_rename)
from src.head_foot import headfooters   

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

dbutils.widgets.text('con_pagos', 'false', "Ejecutar PIS-Payment Info. Sec.")
dbutils.widgets.text('workflow_stub', 'true', "Nombre de workflow como campo en reportes.")
dbutils.widgets.text('specs_local', 'true', "Archivo Feather p. Specs en Repo")


# COMMAND ----------

haz_pagos = (w_get('con_pagos').lower() == 'true')

specs_local = (w_get('specs_local') == 'true')
at_specs = default_path if specs_local else f"{app_path}/specs"
gold_container = app_resourcer.get_storage_client(None, 'gold')

w_stub = (w_get('workflow_stub').lower() == 'true')

falcon_builder = EpicDataBuilder(typehandler=falcon_handler)

datalake = app_resourcer['storage']
dlk_permissions = app_agent.prep_dbks_permissions(datalake, 'gen2')
app_resourcer.set_dbks_permissions(dlk_permissions)


# COMMAND ----------

# MAGIC %md
# MAGIC # Cuentas  
# MAGIC
# MAGIC Se tienen que transformar los tipos de cuenta de acuerdo a los siguientes esquema: 
# MAGIC
# MAGIC ## Para Fiserv
# MAGIC Los siguientes tipos de cuenta se definen en las especificaciones de Excel. 
# MAGIC
# MAGIC | Clave | Descripción                  |
# MAGIC |-------|------------------------------|
# MAGIC | D     | DDA/current account          
# MAGIC | M     | Mortgage
# MAGIC | H     | Home Equity Line of Credit (HELOC)
# MAGIC | S     | Savings
# MAGIC | C     | Credit card
# MAGIC | LU    | Unsecured Loan
# MAGIC | LA    |  Automobile Loan
# MAGIC | LS    | Secured Loan (other than auto/mortgage)
# MAGIC | UC    | Unsecured Line of Credit
# MAGIC | SC    | Secured Line of Credit (other than HELOC)
# MAGIC | MM    | Money market
# MAGIC | T     | Treasury
# MAGIC | Z     | Certificate of deposit
# MAGIC | B     | Brokerage
# MAGIC | O     | Other Deposit Accounts (Annuity, Life Insurance, and so on)
# MAGIC
# MAGIC ## Accounts Tiene  
# MAGIC Los siguientes tipos de productos se obtienen de la tabla de `current_account` como sigue: 
# MAGIC ```python
# MAGIC the_products = (spark.read.table('current_account')
# MAGIC     .select('ProductID', 'ProductDesc')
# MAGIC     .distinct().display())
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

accounts_2 = accounts_1.select_plus(accounts_loader)

accounts_3 = (accounts_2
    .select(accounts_onecol)
    .prep_one_col(header_info=headfooters[('account', 'header')],
                 trailer_info=headfooters[('account', 'footer')]))

accounts_3.display()

accounts_3.save_as_file(
    f"{app_abfss}/reports/accounts/{acct_time}.csv",
    f"{app_abfss}/reports/accounts/tmp_delta",
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

customers_1 = (one_customers(customers_0)
    .join(x_customers(customers_0), on='client_id')
    .with_column_plus(customers_extract['clients'])
    #.with_column_plus(customers_extract['clients_x']) # no existe en blob
    .with_column_plus(customers_extract['_val'])
    .with_column_plus(customers_extract['None'])
    .join(gender_df_2, on='gender').drop('gender')
    .withColumnRenamed('gender_new', 'gender'))

customers_2 = (customers_1
    .select_plus(customers_loader))

customers_3 = (customers_2
    .select(customers_onecol)
    .prep_one_col(header_info=headfooters[('customer', 'header')],
                 trailer_info=headfooters[('customer', 'footer')]))

# customers_3.display()

customers_3.save_as_file(
    f"{app_abfss}/reports/customers/{cust_time}.csv",
    f"{app_abfss}/reports/customers/tmp_delta",
    header=False, ignoreTrailingWhiteSpace=False, ignoreLeadingWhiteSpace=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pagos

# COMMAND ----------

if haz_pagos:
    pymt_time = get_time()
    payments_specs = (pd.read_feather(at_specs/'payments_cols.feather')
            .rename(columns=falcon_rename))
    payments_specs.loc[1, 'column'] = 'modelSTUB' if w_stub else 'RBTRAN'

    one_column = '~'.join(map(row_name, payments_specs.itertuples()))    

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
    .load(f"{app_abfss}/reports/accounts/{cust_time}.csv"))
    
ais_inf = (post_ais
    .select(F.length('_c0').alias('ais_longitud'))
    .groupBy('ais_longitud')
    .count())

ais_inf.display()

# COMMAND ----------

print("Filas CIS-post escritura")
post_cis = (spark.read.format('csv')
    .load(f"{app_abfss}/reports/customers/{cust_time}.csv"))
cis_inf = (post_cis
    .select(F.length('_c0').alias('cis_longitud'))
    .groupBy('cis_longitud')
    .count())
cis_inf.display()

# COMMAND ----------

ais_cnts = ais_inf.collect()[0]["count"]
ais_long = ais_inf.collect()[0]["ais_longitud"]
cis_cnts = cis_inf.collect()[0]["count"]
cis_long = cis_inf.collect()[0]["cis_longitud"]

name = ["AIS","CIS"]
count = [ais_cnts,cis_cnts]
long = [ais_long,cis_long]
color = ["blue","red"]

name2 = []; name3 = []

for i in range(0,len(name),1):
    name2.append(name[i]+" - "+str(count[i]))
    name3.append(name[i]+" - "+str(long[i]))
    
fig,ax = plt.subplots(1,2,figsize = (9,3),sharey = False)
ax[0].bar(name,count,label = name2, color = color)
ax[0].legend()
ax[1].bar(name,long,label = name3,color = color)
ax[1].legend()

plt.show()


# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2. Exploración de archivos

# COMMAND ----------

cis_path = f"{app_abfss}/reports/customers/"
print(f"""
CIS Path:\t{cis_path}
(horario UTC)"""[1:])
(dirfiles_df(cis_path, spark)   
    .loc[:, ['name', 'modificationTime', 'size']])

# COMMAND ----------

ais_path = f"{app_abfss}/reports/accounts/"
print(f"""
AIS Path:\t{ais_path}
(horario UTC)"""[1:])
(dirfiles_df(ais_path, spark)   
    .loc[:, ['name', 'modificationTime', 'size']])
