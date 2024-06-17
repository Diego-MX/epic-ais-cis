# SETUP_KEYS tiene un SERVICE PRINCIPAL. 
# AZURE_RESOURCES: KEYVAULT, STORAGE, STORAGE_PATH[S?], BLOB_PATH
# DBKS_MAPPING tienes tablas que lees del catálogo. 

# La construcción utiliza ENV, SERVER
# construye APP_AGENT, APP_RESOURCER, DBKS_TABLES, BLOB_PATH. 

# 1. El Service Principal conecta. 
# 2. Los recursos de Azure existen y se tienen accesos. 
# 3. Las tablas del mapping se pueden leer. 

import pytest
from pyspark.sql import SparkSession

import config as c


spark = SparkSession.builder.getOrCreate()

# Let the Tests Begiiiin!!! 
def test_databricks_tables_are_accessible(): 
    for tbl_key, tbl_name in c.DBKS_MAPPING.items(): 
        assert spark.catalog.tableExists(tbl_name),\
            f"Tabla para {tbl_key} no existe en catálogo."
    return        


def test_service_principal_is_active():
    pass


def test_azure_resources_are_active(): 
    pass 


