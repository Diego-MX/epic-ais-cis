# Databricks notebook source
# MAGIC %md 
# MAGIC # Tests Falcon
# MAGIC
# MAGIC Lo llamo Tests, aunque en realidad es un dump de las funciones que utilizó un servidor (DX).  
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Check Builder, Widths
# MAGIC
# MAGIC Estas funciones se usaron para checar sus respectivos. 

# COMMAND ----------


def check_builder(build_dict): 
    check_keys = list(x for x in build_dict.keys() if x not in ['_val', 'None'])
    if len(check_keys) == 0: 
        print("Builder is empty.")
    elif len(check_keys) == 1: 
        print(f"Builder can be computed.\nUse {check_keys}.")
    else: 
        print(f"Builder needs JOIN(s).\n{check_keys}.")
    return


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
# MAGIC
# MAGIC ## Move Files
# MAGIC
# MAGIC Al correrlo tenía un error en la dirección, y oploudeaba (del inglés _upload_) los archivos en el DBFS.  
# MAGIC En esta sección se hacen las correcciones.  

# COMMAND ----------

from config import app_resourcer

local_path = f"{app_resourcer['blob_path']}"
upload_path = app_resourcer.get_resource_url('abfss', 'storage', 
        blob_path=local_path, container='gold')
print(f"""
    Ruta local:  {local_path}
    Ruta upload: {upload_path}""")

# COMMAND ----------

local_1 = f"{local_path}/reports"
[x.name for x in dbutils.fs.ls(local_1)]

# COMMAND ----------

[x.name for x in dbutils.fs.ls(upload_path)]

# COMMAND ----------


