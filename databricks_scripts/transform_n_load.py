import sys

sys.path.append("/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/my_lib/")

from util import log_tests
from load import transform_n_load


def transform_and_load():
    log_tests("Transform and Load Test", header=True)
    transform_n_load(
        local_dataset="air_quality.csv",
        new_data_tables={
            "air_quality": {
                "air_quality_id": "INT",
                "fn_indicator_id": "INT",
                "fn_geo_id": "INT",
                "time_period": "STRING",
                "start_date": "STRING",
                "data_value": "FLOAT",
            },
        },
        new_lookup_tables={
            "indicator": {
                "indicator_id": "INT",
                "indicator_name": "STRING",
                "measure": "STRING",
                "measure_info": "STRING",
            },
            "geo_data": {
                "geo_id": "INT",
                "geo_place_name": "STRING",
                "geo_type_name": "STRING",
            },
        },
        column_map={
            "air_quality_id": 0,
            "indicator_id": 1,
            "indicator_name": 2,
            "measure": 3,
            "measure_info": 4,
            "geo_type_name": 5,
            "geo_id": 6,
            "geo_place_name": 7,
            "time_period": 8,
            "start_date": 9,
            "data_value": 10,
            "fn_geo_id": 6,
            "fn_indicator_id": 1,
        },
        on_databricks=True,
    )
    log_tests("Transform and Load Test Successful", last_in_group=True)
    print("Transform and Load Test Successful")


if __name__ == "__main__":
    transform_and_load()
