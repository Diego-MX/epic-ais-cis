# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Pruebas Unitarias - Arquitectura
# MAGIC
# MAGIC Se revisa la arquitectura del repositorio con énfasis en:
# MAGIC    - Scope
# MAGIC    - Token GitHub
# MAGIC    - Service Principal
# MAGIC    - Keyvault
# MAGIC    - Tables
# MAGIC
# MAGIC # Pruebas Unitarias - Repositorio
# MAGIC
# MAGIC Estas pruebas se enfocan en los recursos qeu utiliza el repositorio:
# MAGIC    - Feather accounts local
# MAGIC    - Feather costumers local
# MAGIC    - Feather paymonts local
# MAGIC
# MAGIC # Pruebas Unitarias - Tablas
# MAGIC
# MAGIC Se revisan todos los recursos que alimentan los repositorios, se tiene como objetivo verificar:
# MAGIC    - Ubicación del recurso - ¿Existe?
# MAGIC    - Nombre de columnas
# MAGIC    - Longitud de las columnas
# MAGIC    - Tipo de datos
# MAGIC

# COMMAND ----------

# DBTITLE 1,Unit Test
import sys
import pytest

from function_t import FunctionsTest

sys.dont_write_bytecode = True

retcode = pytest.main([".", "-vv", "-p", "no:cacheprovider"])


tool = FunctionsTest()
tool.response_retcode(retcode)
