# Databricks notebook source
# MAGIC %md
# MAGIC # Descripción
# MAGIC * El cometido es crear muchos _tests_.  
# MAGIC * Investigamos la librería/_framework_ `pytest`.  
# MAGIC * De inicio, no encontramos cómo usar la _web terminal_ para correrlos.  
# MAGIC   Entonces tenemos la opción de ejecutarlos por _notebook_:  
# MAGIC   Ya sea manualmente, o bien desde un _job_ independiente.  

# COMMAND ----------

import pytest

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."


# COMMAND ----------

from config import 
