# Databricks notebook source
from pyspark.sql import functions as F, Row, SparkSession   # pylint: disable=import-error
from pyspark.dbutils import DBUtils 
from azure.keyvault.secrets import (SecretClient, 
                                    KeyVaultSecret,
                                    KeyVaultSecretIdentifier) 

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

dbutils.secrets.list("eh-core-banking")


# COMMAND ----------

a = dbutils.secrets.get(scope = "eh-core-banking",key="aad-tenant-id")
print(a,a)
print(KeyVaultSecretIdentifier("client_id"))
type(dbutils.secrets.get(scope = "eh-core-banking",key="sp-core-events-secret"))


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

import pytest
import sys

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])
print(retcode)
# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
