# Databricks notebook source
# MAGIC %md
# MAGIC # Creación de la tabla dim_client_x

# COMMAND ----------

# DBTITLE 1,EpicPy
import dbks_dependencies as deps

deps.gh_epicpy('meetme-1',  
    tokenfile='/Workspace/Repos/juan.v@bineo.com/fraud-prevention/user_databricks.json', typing=False, verbose=True)

# COMMAND ----------

# DBTITLE 1,Libraries
import os 

from pyspark.sql import SparkSession, dataframe as pys_DataFrame, functions as F
from pyspark.sql.functions import first, col
from pyspark.dbutils import DBUtils

from operator import methodcaller as ϱ
from toolz import pipe, remove
from toolz.curried import map as map_z

from epic_py.tools import dirfiles_df, partial2


# COMMAND ----------

# DBTITLE 1,Variable
visible = False

union = "hash_id"
other = "client_id"

ENV = os.getenv("ENV_TYPE")
ls_eliminados = ['hash_diff', 'load_date_ts', 'load_date', 'end_date', 'source']

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
dict_directorio = {"route":{
                    "original":[f"{ENV}.star_schema.dim_client_new"],
                    "base":[f"{ENV}.data_vault.hub_client"],
                    "data_vault": [f"{ENV}.data_vault.sat_client_attrs",
                            f"{ENV}.data_vault.sat_client_addr",
                            f"{ENV}.data_vault.sat_client_flags",
                            f"{ENV}.data_vault.sat_client_beneficiaries",
                            f"{ENV}.data_vault.sat_client_document_data",
                            f"{ENV}.data_vault.sat_client_onb_attrs",
                            f"{ENV}.data_vault.sat_client_onb_flags",
                            f"{ENV}.data_vault.sat_client_kyc",
                            f"{ENV}.data_vault.sat_client_alias"]
                            }   
                    }

# COMMAND ----------

# DBTITLE 1,Tools
def elimina_columnas(df_data:pys_DataFrame,drop_column:list, keep_column:list = [])-> pys_DataFrame:
    """La Función tiene como tarea principal eliminar columnas de un DataFrame, también se puede salvar 
    guardar ciertas columnas.
    :param df_data: DataFrame al que se le retiran las columnas
    :param drop_column: Lista con los nombres de las columnas a eliminar
    :param keep_column: Lista con los nombres de las columnas a guardar
    """
    for ii in drop_column:
        if not ii in keep_column:
            df_data = df_data.drop(ii)
    return df_data

def columnas_repetidas(df_data_1:pys_DataFrame, df_data_2:pys_DataFrame, columna: str = None)->list:
    """ Función que busca columnas repetidas y no repetidas entrega dos listas respectivamente,
     la primera contiene las columnas que se repiten y la segunda muestra las columnas unicas,
    :param df_data_1: DataFrame que funge como base para la unión
    :param df_data_2: DataFrame que se unira a la primera
    :param columna: Nombre de la columna por la cual se hace la unión de las tablas
    """
    ls_borrados = []; ls_onta = []

    for jj in df_data_2.columns:
        if jj in df_data_1.columns:
            if jj == columna:
                pass
            else:
                ls_borrados.append(jj)
        else:
            ls_onta.append(jj)

    return [ls_borrados, ls_onta]

def union_tablas(ls_route:list, column_union: str, other_column: str, jump_table: str,
                   df_data_0:pys_DataFrame, ls_drop:list = []) -> pys_DataFrame:
    """ Función realiza la condensación de las tablas sí así se requiere y la unión de las mismas
    :param ls_route: Lista con todas las direcciones de las tablas sat_client
    :param column_union: Nombre de la coumna por la cual se unen las tablas
    :param other_column: Nombre de la columna por la cual se hace el conteo de los regitros 
        después de la unión de ambas tablas 
    .param jump_table: Dirección de la tabla que no puede ser condensada
    :param df_data_0: DataFrame de la primera tabla, en ella se llevaran a cabo todas las uniones
    :param ls_drop: Lista de columnas que serán eliminadas de todas las tablas, esto no aplica 
        para la primera tabla
    """

    for ii in ls_route:
        df_table = spark.read.table(ii)

        print(f"Nombre de la tabla: {ii}")
        print(f"Número de registros fuente original: {df_table.count()}")

        if not ii == f"{ENV}.{jump_table}":
            expresion = [first(column,ignorenulls=True).alias(column) for column in df_table.columns if column not in [column_union]]
            df_table = df_table.groupBy(column_union).agg(*expresion)

        print(f"Número de registros información condensada: {df_table.count()}")

        drop_column = columnas_repetidas(df_data_0, df_table,column_union)[0]
        df_table = elimina_columnas(df_table, drop_column)
        df_table = df_data_0.join(df_table, on = column_union, how = "left")

        print(f"Número de registros después de la unión: {df_table.count()}")
        count = df_table.select(other).distinct().count()
        print(f"Número de registros en ID: {count}")
        print("\n")

        if visible:
            df_base_client.display()

        df_data_0 = df_table

    return df_data_0

agg_one = lambda cc: F.any_value(cc).alias(cc)

def clients(df_data_0:pys_DataFrame) -> pys_DataFrame:
    """Esta función crea un DataFrame con las columnas proporcionadas por kyc_answer,
    solo utiliza OCCUPATION, SOURCEOFINCOME
    :parm df_data_0: DataFrame que contiene las columnas respectivas
    """
    kyc_cols = {'OCCUPATION': 'x_occupation', 
            'SOURCEOFINCOME': 'x_src_income'}
    
    kyc_df = (df_data_0
       .select('client_id', 'kyc_id', 'kyc_answer')
       .groupBy('client_id')
       .pivot('kyc_id', list(kyc_cols.keys()))
       .agg(F.first('kyc_answer'))
       .withColumnsRenamed(kyc_cols))
    
    df_data_1 = (df_data_0
        .withColumn('x_address', F.concat_ws(" ", "addr_street", "addr_external_number"))
        .select('client_id', 'x_address')
        .groupBy('client_id').agg(agg_one('x_address'))
        .join(kyc_df, 'client_id', how='left'))
    
    return df_data_1

def one_customers(df_data_0:pys_DataFrame) -> pys_DataFrame:
    first_cols = pipe(df_data_0.columns, 
        partial2(remove, ϱ('startswith', ('client_id', 'ben_', 'kyc_')), ...), 
        map_z(agg_one))
    df_data_1 = df_data_0.groupBy('client_id').agg(*first_cols)
    return df_data_1

# COMMAND ----------

# MAGIC %md
# MAGIC # Armado de la tabla
# MAGIC En las siguientes celdas se construye la tabla de __dim_client__, es importante mencionar que hay diferentes versiones de la tabla por lo que sin tener documentación veraz que fundamente la creación de esta nueva tabla, se evita eliminar columnas y se obta por unir todas y cada una de las tablas por medio de **hash_id**, por ello esta versión de tabla tiene más de 100 columnas.

# COMMAND ----------

# MAGIC %md
# MAGIC Tabla base, en ella se realizarán todas las uniones correspondientes

# COMMAND ----------

# DBTITLE 1,df_base_client
route_client = dict_directorio["route"]["base"][0]

print("Nombre de la tabla: {}".format(route_client))

df_base_client = spark.read.table(route_client)
df_base_client = df_base_client.withColumnRenamed("bk_id","client_id")

print("Número de registros: {}".format(df_base_client.count()))

if visible:
    df_base_client.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Tablas varias, son tablas que se unen a la tabla base crendo la tabla dim_client, después de unir todas las tablas se hace una expanción de la 
# MAGIC  columna kyc_answer con la finalidad de obtener esa información

# COMMAND ----------


dict_route = dict_directorio["route"]["data_vault"]
not_column = "data_vault.sat_client_kyc"

df_final = union_tablas(dict_route, union, other, not_column, df_base_client, ls_eliminados)
df_final = one_customers(df_final).join(clients(df_final), on=other)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Escritura de datos

# COMMAND ----------

# DBTITLE 1,Table Variable
create_table_delta = False
create_table_catalog = False
drop_table = False

table_type = "delta"
mode_write = "overwrite"
path_data = "abfss://bronze@stlakehyliaqas.dfs.core.windows.net/ops/core-banking-x/dim_client/data"


# COMMAND ----------

# DBTITLE 1,Create delta table
if create_table_delta:
    df_final.write.format(table_type).mode(mode_write).save(path_data)

# COMMAND ----------

# DBTITLE 1,Create catalog table
if create_table_catalog: 
    create_clause = "CREATE TABLE {} \nUSING DELTA LOCATION \"{}\";"
    account_name = f"{ENV}.star_schema.dim_client_x"
    print(    create_clause.format(account_name, path_data))
    spark.sql(create_clause.format(account_name, path_data))

# COMMAND ----------

# DBTITLE 1,Drop tables
dbutils = DBUtils(spark)
if drop_table:
    dbutils.fs.rm(path_data, recurse=True)
    account_name = f"{ENV}.star_schema.dim_client_x"
    # Usar SQL para eliminar la tabla Delta
    spark.sql(f"DROP TABLE IF EXISTS {account_name}")

