# Databricks notebook source
# MAGIC %md
# MAGIC # Descripción
# MAGIC * El cometido es crear muchos _tests_.  
# MAGIC * Investigamos el _framework_ `pytest`.  
# MAGIC * De inicio no encontramos cómo usar la _web terminal_ para correrlos.  
# MAGIC   Entonces los ejecutamos como _notebook_:  
# MAGIC   primero manualmente, y más adelante desde un _job_ independiente.  

# COMMAND ----------

import pytest
from subprocess import check_call
from sys import dont_write_bytecode

dont_write_bytecode = True

retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])
assert retcode == 0, "The pytest invocation failed. See the log for details."


