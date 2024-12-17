import sys
from util import log_tests
from load import static_data
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DateType,
    FloatType,
)

sys.path.append("/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/my_lib/")


def get_a_session():
    spark = SparkSession.builder.appName("CreateDataFrameWithSchema").getOrCreate()
    for key, value in static_data.spark_dataframes.items():
        value.createOrReplaceTempView(key)
    return spark


def get_table_columns(table_name: str):
    spark = get_a_session()
    log_tests(f"Getting columns for {table_name}")
    columns = spark.table(table_name).columns
    # spark.stop()
    return columns


def get_primary_key(table_name):
    return get_table_columns(table_name)[0]


def read_data(table_name: str, data_id: int):
    spark = get_a_session()
    result = spark.sql(
        f"select * from {table_name} where {get_primary_key(table_name)} = {data_id}"
    )
    # spark.stop()
    if result:
        return result.first()
    else:
        return None


def read_all_data(table_name: str):
    spark = get_a_session()
    to_execute = f"select * from {table_name}"
    log_tests("Executing query...")
    log_tests(to_execute, issql=True)
    result = spark.sql(to_execute)
    # spark.stop()
    if result:
        return result.collect()
    else:
        return None


def execute_read_query(query_to_execute: str):
    spark = get_a_session()
    log_tests("Executing custom query...")
    log_tests(query_to_execute, issql=True)
    result = spark.sql(query_to_execute)
    # spark.stop()
    if result:
        return result
    else:
        return None


def get_col_type(col_type: str):
    if col_type.lower() == "int":
        return IntegerType()
    elif col_type.lower() == "string":
        return StringType()
    elif col_type.lower() == "date":
        return DateType()
    elif col_type.lower() == "float":
        return FloatType()


def save_data(table_name: str, row: list):
    spark = get_a_session()
    newRow = spark.createDataFrame(row, spark.table(table_name).schema)
    log_tests(f"Adding new record {row}")
    static_data.spark_dataframes[table_name] = spark.table(table_name).union(newRow)
    # spark.stop()
    return "Save Successful"


def delete_data(table_name: str, data_id: int):
    spark = get_a_session()

    log_tests(f"Filtering out record with id {data_id}")
    static_data.spark_dataframes[table_name] = spark.table(table_name).filter(
        spark.table(table_name)[get_primary_key(table_name)] != data_id
    )

    # spark.stop()

    return "Delete Successful"
