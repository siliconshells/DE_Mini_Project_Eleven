import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    FloatType,
)
import csv

import sys

sys.path.append("/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/my_lib/")

from util import log_tests


class static_data:
    spark_dataframes = {}


def get_col_type(col_type: str):
    if col_type.lower() == "int":
        return IntegerType()
    elif col_type.lower() == "string":
        return StringType()
    elif col_type.lower() == "date":
        return DateType()
    elif col_type.lower() == "float":
        return FloatType()


def create_dataframe_with_schema(spark, table, columns):
    # create schema
    schema = StructType([])
    for column, itstype in columns.items():
        schema.fields.append(StructField(column, get_col_type(itstype)))

    # create dataframe
    static_data.spark_dataframes[table] = spark.createDataFrame([], schema)


# load the csv file and store in shared memory
def transform_n_load(
    local_dataset: str,
    new_data_tables: dict,
    new_lookup_tables: dict,  # dict of dict - {name :{column:type},name :{column:type}}
    column_map: dict,
    on_databricks=False,
):
    """ "Transforms and Loads data into the PySpark DataFrames"""

    # load the data from the csv
    reader = csv.reader(
        open(
            (
                "/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/data/"
                if on_databricks
                else "./data/"
            )
            + local_dataset,
            newline="",
        ),
        delimiter=",",
    )

    # Initialize SparkSession
    spark = SparkSession.builder.appName("CreateDataFrameWithSchema").getOrCreate()

    temp_list_of_tuples = {}
    # Create dataframes with schema
    for table, columns in new_data_tables.items():
        log_tests(f"Creating non-lookup table: {table}")
        create_dataframe_with_schema(spark, table, json.loads(json.dumps(columns)))
        temp_list_of_tuples[table] = []

    for table, columns in new_lookup_tables.items():
        log_tests(f"Creating lookup table: {table}")
        create_dataframe_with_schema(spark, table, json.loads(json.dumps(columns)))
        temp_list_of_tuples[table] = []

    log_tests("Tables created.")

    # skip the first row
    log_tests("Skipping the first row...")
    next(reader)
    log_tests("Inserting table data...")
    for row in reader:
        first_for_loop_broken = False
        for table, columns in new_lookup_tables.items():
            first_col_name_in_columns = next(
                iter(columns)
            )  # get the first key in columns

            # If the ID is not a number don't import it
            if not row[column_map[first_col_name_in_columns]].isnumeric():
                first_for_loop_broken = True
                break  # Go to outer loop

            result = sum(
                1
                for atuple in temp_list_of_tuples[table]
                if atuple[0] == row[column_map[first_col_name_in_columns]]
            )
            if result == 0:
                data_values = [(row[column_map[col]]) for col in columns]
                temp_list_of_tuples[table].append(tuple(data_values))

        # Only load the data if all lookup information are there
        if not first_for_loop_broken:
            for table, columns in new_data_tables.items():
                data_values = [(row[column_map[col]]) for col in columns]
                temp_list_of_tuples[table].append(tuple(data_values))
    log_tests("Inserting table data completed")

    for key, value in temp_list_of_tuples.items():
        static_data.spark_dataframes[key] = static_data.spark_dataframes[
            key
        ].unionByName(
            spark.createDataFrame(value, static_data.spark_dataframes[key].columns)
        )

    # Do columns again for int and float they complain and cause merge errors
    static_data.spark_dataframes["geo_data"] = static_data.spark_dataframes[
        "geo_data"
    ].withColumn(
        "geo_id", static_data.spark_dataframes["geo_data"]["geo_id"].cast(IntegerType())
    )

    static_data.spark_dataframes["air_quality"] = static_data.spark_dataframes[
        "air_quality"
    ].withColumn(
        "fn_indicator_id",
        static_data.spark_dataframes["air_quality"]["fn_indicator_id"].cast(
            IntegerType()
        ),
    )

    static_data.spark_dataframes["air_quality"] = static_data.spark_dataframes[
        "air_quality"
    ].withColumn(
        "air_quality_id",
        static_data.spark_dataframes["air_quality"]["air_quality_id"].cast(
            IntegerType()
        ),
    )

    static_data.spark_dataframes["air_quality"] = static_data.spark_dataframes[
        "air_quality"
    ].withColumn(
        "fn_geo_id",
        static_data.spark_dataframes["air_quality"]["fn_geo_id"].cast(IntegerType()),
    )

    static_data.spark_dataframes["air_quality"] = static_data.spark_dataframes[
        "air_quality"
    ].withColumn(
        "data_value",
        static_data.spark_dataframes["air_quality"]["data_value"].cast(FloatType()),
    )

    # Saving a table name of indicator will fail
    static_data.spark_dataframes["indicator"] = static_data.spark_dataframes[
        "indicator"
    ].withColumn(
        "indicator_id",
        static_data.spark_dataframes["indicator"]["indicator_id"].cast(IntegerType()),
    )

    # save data

    static_data.spark_dataframes["geo_data"].write.format("delta").mode(
        "overwrite"
    ).saveAsTable("tb_geo_data")
    static_data.spark_dataframes["air_quality"].write.format("delta").mode(
        "overwrite"
    ).saveAsTable("tb_air_quality")
    static_data.spark_dataframes["indicator"].write.format("delta").mode(
        "overwrite"
    ).saveAsTable("tb_indicator")

    static_data.spark_dataframes["indicator"].show()
    static_data.spark_dataframes["geo_data"].show()
    static_data.spark_dataframes["air_quality"].show()

    # spark.stop()

    return "Transform  and load Successful"
