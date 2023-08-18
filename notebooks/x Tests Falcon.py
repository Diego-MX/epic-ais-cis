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

# ... 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Move Files
# MAGIC
# MAGIC Al correrlo tenía un error en la dirección, y oploudeaba (del inglés _upload_) los archivos en el DBFS.  
# MAGIC En esta sección se hacen las correcciones.  

# COMMAND ----------

from importlib import reload
import config; reload(config)

from config import app_resourcer, app_agent

local_path = f"{app_resourcer['blob_path']}"
upload_path = app_resourcer.get_resource_url('abfss', 'storage', 
        blob_path=local_path, container='gold')
print(f"""
    Ruta local:  {local_path}
    Ruta upload: {upload_path}""")
