"""
Pruebas unitarias para revisar los recursos que alimentan 
al repositorio, estas pruebas tienen como finalidad verificar:
- Que las tablas existan en el DataVault
- Las columnas de las tablas no cambien y se mantengan sin cambios
- Los tipos de datos de las columnas son consistentes  
"""
from pyspark.sql import SparkSession, DataFrame

import pytest

from config import ENV # pylint: disable = import-error
from function_t import FunctionsTest # pylint: disable = import-error
from config_t import TABLES, REPOSITORY # pylint: disable = import-error

spark = SparkSession.builder.getOrCreate()
REPO = TABLES[REPOSITORY]

class TestTables():
    """Pruebas unitarias para todos los insumos que alimentan al repositorio
    se prueba la existencia de las tablas, consistencia en el nombre de las 
    columnas, tipos de datos y si la tabla es compatible con DataFrame
    """

    @pytest.fixture
    def import_function(self):
        """Se manda llamar la calse FucntionsTest"""
        return FunctionsTest()

    def test_table_exist(self)->None:
        """La función tiene como objetivo verificar que la tabla exista"""

        for key, content in REPO.items():
            for table in content:
                table_exist = spark.catalog.tableExists(f"{ENV}.{key}.{table}")
                assert table_exist, f"La tabla {ENV}.{key}.{table} no existe"

    def test_table_compatible(self)->None:
        "La prueba verifica que la tabla se pueda llamar en formato DataFrame"

        for key, content in REPO.items():
            for table in content:
                data = spark.read.table(f"{ENV}.{key}.{table}")
                assert isinstance(data,DataFrame), f"La tabla no es DataFrame: {ENV}.{key}.{table}"

    def test_table_column(self,import_function)->None:
        """Verifica las columnas sean iguales"""

        data_json = import_function.read_json()
        reference = data_json[REPOSITORY]["columns"]

        for key, content in REPO.items():
            for table in content:
                data = spark.read.table(f"{ENV}.{key}.{table}")
                its_correct = reference[table] == data.columns
                assert its_correct, import_function.deferences_columns(reference,
                        table,data.columns)

    def test_type_data(self,import_function)->None:
        """Verifica que el tipo de las columnas sea consistente"""

        data_json = import_function.read_json()
        reference = data_json[REPOSITORY]["data_type"]

        for key, content in REPO.items():
            for table in content:
                data = spark.read.table(f"{ENV}.{key}.{table}")
                its_correct = reference[table] == dict(data.dtypes)
                assert its_correct, import_function.deferences_types(reference,
                        table,data)

# Finite Incantatem
