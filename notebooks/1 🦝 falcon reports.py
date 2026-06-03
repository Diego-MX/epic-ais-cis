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

%pip install /Workspace/Users/diego.v@bineo.com/wheels/epic_py/epic_py-1.2.1-133dbr-cp310-none-any.whl
# deps.gh_epicpy('meetme-1', tokenfile='../user_databricks.json', typing=False, verbose=True)

# COMMAND ----------

from datetime import datetime as dt
from io import BytesIO
from operator import methodcaller as ϱ
from pytz import timezone as tz

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import DataFrame, functions as F, Row, SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.dbutils import DBUtils
from toolz import remove
from toolz.curried import map as map_z

from epic_py.delta import EpicDF, EpicDataBuilder, TypeHandler
from epic_py.tools import dirfiles_df, partial2, thread

from src import (app_agent, app_resourcer, app_abfss, app_path,
    dbks_tables, falcon_types, falcon_rename)
from src.head_foot import headfooters

from config import ENV    # PARCHE MOMENTANEO DADO QUE LAS TABLAS SE MUEVEN >:l

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
falcon_handler = TypeHandler(falcon_types)

# COMMAND ----------

COL_DEBUG = False

w_get = dbutils.widgets.get

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
    return dt.now(tz=tz(a_tz)).strftime(format=time_fmt)

date_str = lambda ss: dt.strptime(ss, '%Y-%m-%d').date()

# dates_by_env = dict(qas='2022-01-01', prd='2023-05-01', _='2023-01-01')
# Ya no se usa. 
default_path = "../refs/upload-specs"

dbutils.widgets.text('con_pagos', 'false', "Ejecutar PIS-Payment Info. Sec.")
dbutils.widgets.text('workflow_stub', 'true', "Nombre de workflow como campo en reportes.")
dbutils.widgets.text('specs_local', 'true', "Archivo Feather p. Specs en Repo")


# COMMAND ----------

haz_pagos = w_get('con_pagos').lower() == 'true'

specs_local = w_get('specs_local') == 'true'
at_specs = default_path if specs_local else f"{app_path}/specs"
gold_container = app_resourcer.get_storage_client(None, 'gold')

w_stub = w_get('workflow_stub').lower() == 'true'

falcon_builder = EpicDataBuilder(typehandler=falcon_handler)

datalake = app_resourcer['storage']
dlk_permissions = app_agent.prep_dbks_permissions(datalake, 'gen2')
app_resourcer.set_dbks_permissions(dlk_permissions)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuentas  
# MAGIC
# MAGIC Se tienen que transformar los tipos de cuenta de acuerdo a los siguientes esquema: 
# MAGIC
# MAGIC ### Para Fiserv
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
# MAGIC ### Accounts Tiene  
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

# DBTITLE 1,Parche header
def repair_dbks(df_last:DataFrame, df_ancestor:DataFrame, configuration:dict)-> DataFrame:
    """ La función solventa el movimiento del header en AIS después de la escritura, se extraen los
    headers y footers de la última transformación y se obtienen los datos por separado para unirlos 
    utilizando DataFrame de Pandas y postrtiormente convertirlos a un DataFrame de Spark.
    :params DataFrame df_last: Es el Dataframe que contiene header, data y footer - accounts_3 -,
    :params DataFrame df_ancestor: Es la penúltima transformación - accounts_2 -,
    :params Dict configuration: La configuración de la columna única """ 

    df_accounts = df_ancestor.select(configuration).toPandas()

    header = df_last.head(1)[0] 
    footer = df_last.tail(1)[0]
 
    df_header = spark.createDataFrame([header], df_last.columns).toPandas()
    df_footer = spark.createDataFrame([footer], df_last.columns).toPandas()

    df_combinate = pd.concat([df_header, df_accounts], ignore_index=True)
    df_finally = pd.concat([df_combinate, df_footer], ignore_index=True)
    df_finally = spark.createDataFrame(df_finally)

    return EpicDF(df_finally)

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

accounts_2 = accounts_1.select_plus(accounts_loader)

accounts_3 = (accounts_2
    .select(accounts_onecol)
    .prep_one_col(header_info=headfooters[('account', 'header')],
                 trailer_info=headfooters[('account', 'footer')]))

accounts_4 = repair_dbks(accounts_3,accounts_2,accounts_onecol)

print(accounts_0.count(),accounts_1.count(),accounts_2.count(),
      accounts_3.count(),accounts_4.count())

accounts_4.display()

accounts_4.save_as_file(
    f"{app_abfss}/reports/accounts/{acct_time}.csv",
    f"{app_abfss}/reports/accounts/tmp_delta",
    header=False, ignoreTrailingWhiteSpace=False, ignoreLeadingWhiteSpace=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultados

# COMMAND ----------

# MAGIC %md
# MAGIC ###   1.  Longitud de filas

# COMMAND ----------

print("Filas AIS-post escritura")
post_ais = (spark.read.format('csv')
    .load(f"{app_abfss}/reports/accounts/{acct_time}.csv"))

ais_inf = (post_ais
    .select(F.length('_c0').alias('ais_longitud'))
    .groupBy('ais_longitud')
    .count())

ais_inf.display()
print("Primera fila AIS-post", post_ais.first())
print("Última fila AIS-post", post_ais.tail(1)[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Exploración de archivos

# COMMAND ----------

ais_path = f"{app_abfss}/reports/accounts/"
print(f"""
AIS Path:\t{ais_path}
(horario UTC)"""[1:])
(dirfiles_df(ais_path, spark)
    .loc[:, ['name', 'modificationTime', 'size']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clientes
# MAGIC
# MAGIC La columna original para tomar la información del cliente era `gld_client_file`.  
# MAGIC Esta fue eliminada sin aviso y por eso empezó a fallar todo.  
# MAGIC La tabla equivalente es `star_schema.dim_client`.  
# MAGIC Aunque se supone que tiene más estructura que la anterior, 
# MAGIC la realidad es que no está bien hecha.  
# MAGIC
# MAGIC El _hack_ se compone de lo siguiente:  
# MAGIC * Mapeo de columnas `prep_columns`.  
# MAGIC * Un increíble _pivoteo_ de columnas de `kyc`.  
# MAGIC * Un filtrado de datos repetidos debido al desmadre que se hizo con `kyc`.  
# MAGIC
# MAGIC

# COMMAND ----------

agg_one = lambda cc: F.any_value(cc).alias(cc)

def one_customers(df_0):
    """Se retiran los usuarios repetidos por los beneficiarios,
    :param df_0: DataFrame con los datos de interes"""
    first_cols = thread(df_0.columns,
        (remove, ϱ('startswith', ('client_id', 'ben_', 'kyc_')), ...),
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
    df_clean = df_data.select([
        regexp_replace(col(column), '"',"").alias(column) 
        for column in df_data.columns])
    df_return = df_clean.select([
        regexp_replace(col(column), ",", "").alias(column) 
        for column in df_clean.columns])
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

name_onecol = '~'.join(row_name(rr)
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

customers_2 = (customers_1
    .select_plus(customers_loader))

customers_3 = (customers_2
    .select(customers_onecol)
    .prep_one_col(header_info=headfooters[('customer', 'header')],
                 trailer_info=headfooters[('customer', 'footer')]))

print(customers_0.count(),customers_1.count(),
      customers_2.count(),customers_3.count())
customers_3.display()

customers_3.save_as_file(
    f"{app_abfss}/reports/customers/{cust_time}.csv",
    f"{app_abfss}/reports/customers/tmp_delta",
    header=False, ignoreTrailingWhiteSpace=False, ignoreLeadingWhiteSpace=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultados

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Longitud de filas

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Exploración de Archivos

# COMMAND ----------

cis_path = f"{app_abfss}/reports/customers/"
print(f"""
CIS Path:\t{cis_path}
(horario UTC)"""[1:])

(dirfiles_df(cis_path, spark)
    .loc[:, ['name', 'modificationTime', 'size']])


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
# MAGIC ## Resultados Gráficos

# COMMAND ----------

# ais_cnts = ais_inf.collect()[0]["count"]
# ais_long = ais_inf.collect()[0]["ais_longitud"]
# cis_cnts = cis_inf.collect()[0]["count"]
# cis_long = cis_inf.collect()[0]["cis_longitud"]

# name = ["AIS","CIS"]
# count = [ais_cnts, cis_cnts]
# long = [ais_long, cis_long]
# color = ["#f57c10","#17202a"]
# name2 = []# "COUNT" "LONG"

# for i in range(0,len(name),1):
#     name2.append(str(name[i])+" -> "+str(count[i])+" -> "+str(long[i]))

# fig, ax = plt.subplots(figsize = (3,5))

# plt.title("AIS & CIS")
# plt.bar(name, count, label = name2, color = color, width = 1)
# plt.grid(color = "black", linestyle= ":", linewidth = 0.2, which = "major")
# plt.ylabel("Counts -> Accounts & Customers")
# plt.xlabel("Name -> Counts -> Longitude")
# plt.legend()
# plt.show()
