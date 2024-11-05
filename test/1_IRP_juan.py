# Databricks notebook source
# MAGIC %md
# MAGIC # IRP_Juan
# MAGIC
# MAGIC Este _notebook_ funciona como el espacio para poder hacer mis **investigaciones**, **reparaciones** y **pruebas** por eso se llama IRP

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Trabajando andamos ...
# MAGIC Epicpy dado que la versión de spark que contiene el pyspark es obsoleto cuando se ingresa un pyspark actual se rompe y causa situaciónes no deseables.
# MAGIC

# COMMAND ----------

# %pip install databricks-sdk --upgrade 
# dbutils.library.restartPython()

# COMMAND ----------

"""DX, September 5th, 2023
Main object is EpicDF which extends spark.DataFrame functionality. 
"""
from collections import OrderedDict
from datetime import date
from functools import reduce
from operator import add, and_, itemgetter, methodcaller as ϱ, or_
from typing import Union
from warnings import warn
import os

# from databricks.connect.sdk.runtime import spark
from delta.tables import DeltaTable
from pandas import DataFrame as pd_DF
from pyspark.sql import (functions as F, GroupedData, DataFrame as SpkDF, 
    SparkSession)
import pyspark
from packaging.version import parse as v_parse
# from toolz import compose, identity, juxt, pipe
# from toolz.curried import map as map_z

# from ..tools import partial2    # MatchCase, packed, unpacked 
# from ..tools.vi_spk_tools import as_column
# from .i_table_info import FlatFileInfo

# pylint: disable=inherit-non-class
# pylint: disable=useless-return

TYPEHANDLER_WARN = "TYPEHANDLER automatically set with default options."
SPKSESSION_WARN  = "Calling DF.SQL_CTX instead of DF.SPARKSESSION"


# COMMAND ----------


class EpicMixin:    # pylint: disable=missing-class-docstring
    class_map = {}
    @classmethod
    def update_map(cls, map_dict):
        cls.class_map.update(map_dict)

    @classmethod
    def to_epic(cls, obj):
        if type(obj) in cls.class_map:
            epic_class = cls.class_map[type(obj)]
            return epic_class(obj)

        if callable(obj):   # pylint: disable=no-else-return
            e_caller = lambda *a, **kw: cls.to_epic(obj(*a, **kw))
            return e_caller
        else:
            return obj

def parche_version_pyspark(version):
    "Funcionará... lo averiguaremos"
    if v_parse(version.version) < v_parse("3.4.0"):
        comparador = SparkSession
    elif v_parse(version.version) >= v_parse("3.4.0"):
        comparador = version
    return comparador

def is_session(an_obj):
    comparador = parche_version_pyspark(an_obj)
    print(comparador,type(comparador),type(an_obj))
    return type(an_obj) == type(comparador)

class EpicDF(EpicMixin, SpkDF):
    '''Improve Spark Dataframe functionality.'''
    __module__ = 'epic_py'

    def __init__(self, *args):
        df = self._init_args(*args)     # pylint: disable=invalid-name
        try:
            sql_ctx = df.sparkSession
        except AttributeError:
            warn(SPKSESSION_WARN)
            sql_ctx = df.sql_ctx
        super().__init__(df._jdf, sql_ctx)
        self._df = df

        for m_name in self.df_methods:
            a_method = getattr(self, m_name)
            e_method = self.to_epic(a_method)
            setattr(self, m_name, e_method)

    def __getattr__(self, a_name):
        an_attr = super().__getattr__(a_name)
        return self.to_epic(an_attr)

    def __repr__(self):
        return super().__repr__().replace('DataFrame', 'EpicDF')

    def _init_args(self, *args):
        if isinstance(args[0], SpkDF):
            return args[0]
        
        entonces = parche_version_pyspark(args[0])

        if is_session(entonces):
            spark = args[0]
            if isinstance(args[1], pd_DF):
                return spark.createDataFrame(args[1])
            print(1)
            if os.path.exists(os.path.join(args[1],"_delta_log")):
                the_df = (spark.read.format('delta').load(args[1]))
                return the_df
            try:
                print(2)
                the_df = spark.read.table(args[1])
                the_df.display()
                return the_df
            except Exception as exc:    # pylint: disable=broad-exception-caught
                print(f"Exception of type {type(exc)}")
            raise ValueError("Needs pd-DF, Δ-path, table with SparkSession.")
        else:
            raise ValueError("First Argument must be (spark)-DataFrame or SparkSession.")



# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
RUTA = "qas.star_schema.current_account_x"

EpicDF(spark,RUTA)

# COMMAND ----------

asi = spark.read.table(RUTA)

# if DeltaTable.isDeltaTable(spark,RUTA): # Falla no se puede utilizar
#     print(0)

# if spark.read.format("delta").load(RUTA):
#     print(spark.read.format("delta").load(RUTA)) # Falla no se puede usar

import os

review = os.path.join(RUTA,"_delta_log")
if os.path.exists(os.path.join(RUTA,"_delta_log")):
    print(2)
else: 
    print(3)



