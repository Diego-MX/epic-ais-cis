"""Pruebas unitarias, se encargan de verificar que todos se encuentre habilitado y funcional
"""

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

import pandas as pd
import pytest

import config as cfg
from function_test import FunctionsTest

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class TestRepository:
    """Pruebas unitarias correspondientes al repositorio"""

    @pytest.fixture
    def functions_test(self):
        """Llamado de la librería de function_test"""
        return FunctionsTest()

    def test_feather_accounts_local(self,functions_test):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        # Obtención de datos provenientes del archivo feather
        folder = functions_test.get_root()
        data_feather = pd.read_feather(folder[0].path)
        columns_feather = data_feather["columna"].tolist()

        print(columns_feather)

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        name_table = cfg.DBKS_MAPPING["accounts"]
        data_table = spark.read.table(cfg.ENV+"."+name_table)
        columns_table = data_table.columns

        print(columns_table)

        l_save = []

        for column in columns_feather:
            if column in columns_table:
                l_save.append(column)

        assert len(l_save)==2,"No se encontraron coincidencias en las columnas en AccountsLocal"

    def test_feather_costumers_local(self,functions_test):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        # Obtención de datos provenientes del archivo feather
        folder = functions_test.get_root()
        data_feather = pd.read_feather(folder[1].path)
        columns_feather = data_feather["columna"].tolist()

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        name_table = cfg.DBKS_MAPPING["clients"]
        data_table = spark.read.table(cfg.ENV+"."+name_table)
        columns_table = data_table.columns

        l_save = []
        l_special = ["addr_street","addr_external_number","kyc_id","kyc_answer"]

        for column in columns_feather:
            if column in columns_table or column in l_special:
                l_save.append(column)
            elif column == "x_address":
                for i in range(0,2,1):
                    l_save.append(l_special[i])
            elif column in {"x_occupation", "x_src_income"}:
                for i in range(2,4,1):
                    l_save.append(l_special[i])

        assert len(l_save)==18, "No se encontraron coincidencias en las columnas"

    def test_feather_paymonts_local(self, functions_test):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        # Obtención de datos provenientes del archivo feather
        folder = functions_test.get_root()
        data_feather = pd.read_feather(folder[2].path)
        columns_feather = data_feather["columna"].tolist()

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        # No se cuenta con una tabla símil en databricks

        assert columns_feather!=[],"No se encontro el feather paymonts"

# Finite Incatatem
