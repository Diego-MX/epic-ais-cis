"""
Pruebas unitarias setup, 
Estas pruebas tienen como finalidad de revisar que los insumos especificos del repositorio
se encuentren habilitados y listos para ser utilizados.
"""

import warnings

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

import pandas as pd
import pytest

from function_t import FunctionsTest # pylint:disable = import-error
import config as cfg # pylint: disable = import-error
import config_t as cfg_t # pylint: disable = import-error

warnings.filterwarnings("ignore", category=DeprecationWarning)

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class TestRepository:
    """Pruebas unitarias correspondientes al repositorio"""

    @pytest.fixture
    def import_function(self):
        "Llamado de la clase FunctionsTest"
        return FunctionsTest()

    def test_feather_accounts_local(self,import_function):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        # Obtención de datos provenientes del archivo feather
        user = import_function.get_user_note()
        data_feather = pd.read_feather(f"file:/Workspace/Repos/{user}"
                                       f"/{cfg_t.PATHS[cfg.ENV]}/refs/upload-specs/accounts_cols.feather")
        columns_feather = data_feather["columna"].tolist()

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        name_table = cfg.DBKS_MAPPING["accounts"]
        data_table = spark.read.table(cfg.ENV+"."+name_table)
        columns_table = data_table.columns

        l_save = []

        for column in columns_feather:
            if column in columns_table:
                print(column)
                l_save.append(column)

        assert len(l_save)==2,"No se encontraron coincidencias en las columnas en AccountsLocal"

    def test_feather_costumers_local(self,import_function):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        user = import_function.get_user_note()
        # Obtención de datos provenientes del archivo feather
        data_feather = pd.read_feather(f"file:/Workspace/Repos/{user}"
                                       f"/{cfg_t.PATHS[cfg.ENV]}//refs/upload-specs/customers_cols.feather")
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

        assert len(l_save)==15, "No se encontraron coincidencias en las columnas"

    def test_feather_paymonts_local(self,import_function):
        """Se verifica que la tabla contenga las columnas que índica el feather"""
        user = import_function.get_user_note()
        # Obtención de datos provenientes del archivo feather
        data_feather = pd.read_feather(f"file:/Workspace/Repos/{user}"
                                       f"/{cfg_t.PATHS[cfg.ENV]}//refs/upload-specs/payments_cols.feather")
        columns_feather = data_feather["columna"].tolist()

        # Extracción del nombre de las columnas de las tablas. Vienen de DBCKS
        # No se cuenta con una tabla símil en databricks

        assert columns_feather!=[],"No se encontro el feather paymonts"

# Finite Incantatem
