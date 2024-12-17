from my_lib.extract_data import extract
from my_lib.load import transform_n_load
from my_lib.util import log_tests
import os
from dotenv import load_dotenv


from my_lib.lib import (
    read_data,
    read_all_data,
    save_data,
    delete_data,
    get_table_columns,
)

column_map = {
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
}


# Test extract
def test_extract():
    log_tests("Extraction Test", header=True, new_log_file=True)
    log_tests("Removing existing CSV file exists")
    if os.path.exists("data/air_quality.csv"):
        os.remove("data/air_quality.csv")

    log_tests("Confirming that CSV file doesn't exists...")
    assert not os.path.exists("population_bar.png")
    log_tests("Test Successful")

    log_tests("Extracting data and saving...")
    extract(
        "https://data.cityofnewyork.us/resource/c3uy-2p5r.csv?$limit=200000",
        "air_quality.csv",
    )

    log_tests("Testing if CSV file exists...")
    assert os.path.exists("data/air_quality.csv")
    log_tests("Extraction Test Successful", last_in_group=True)
    print("Extraction Test Successful")


# Test transform and load
def test_transform_and_load():
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
    )
    log_tests("Transform and Load Test Successful", last_in_group=True)
    print("Transform and Load Test Successful")


# Test read data
def test_read_data():
    log_tests("One Record Reading Test", header=True)
    row = read_data("air_quality", 740885)
    data_value = 5
    log_tests("Asserting that row[0][data_value] == 16.4")
    assert float(row[data_value]) == 16.4
    log_tests("Assert Successful")
    log_tests("One Record Reading Test Successful", last_in_group=True)
    print("One Record Reading Test Successful")


# Test read all data
def test_read_all_data():
    log_tests("All Records Reading Test", header=True)
    rows = read_all_data("air_quality")
    log_tests("Asserting that len(rows) == 18016")
    assert len(rows) == 18016
    log_tests("All Records Reading Test Successful", last_in_group=True)
    print("All Records Reading Test Successful")


# Test save data
def test_save_data():
    log_tests("Record Saving Test", header=True)

    log_tests("Asserting there's no record in geo_data with ID 100000")
    result = read_data("geo_data", 100000)
    assert result is None
    log_tests("Assert Successful")

    log_tests("Saving new record with ID 100000")
    save_data("geo_data", [(100000, "Lancaster", "UFO")])

    log_tests("Asserting there's now a record in geo_data with ID 100000")
    result = read_data("geo_data", 100000)
    assert result[0] == str(100000)
    log_tests("Assert Successful")

    log_tests("Record Saving Test Successful", last_in_group=True)
    print("Record Saving Test Successful")


# Test delete data
def test_delete_data():
    log_tests("Record Deletion Test", header=True)

    log_tests("Asserting there's a record in geo_data for row ID 100000")
    result = read_data("geo_data", 100000)
    assert result[0] == str(100000)
    log_tests("Assert Successful")

    log_tests("Deleting record with ID 100000")
    print(delete_data("geo_data", 100000))

    log_tests("Asserting there's no record in geo_data with ID 100000")
    result = read_data("geo_data", 100000)
    assert result is None
    log_tests("Assert Successful")

    log_tests("Record Deletion Test Successful", last_in_group=True)
    print("Record Deletion Test Successful")


# Test read all column names
def test_get_table_columns():
    log_tests("Reading All Column Test", header=True)

    columns = get_table_columns("air_quality")

    log_tests("Asserting the air_quality table has six (6) columns")
    assert len(columns) == 6
    log_tests("Assert Successful")

    log_tests("Reading All Column Test Successful", last_in_group=True)
    print("Reading All Column Test Successful")


# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "/tmp/MH_COVID.csv"
url = f"https://{server_h}/api/2.0"


# # Function to check if a file path exists and auth settings still work
# def check_filestore_path(path, headers):
#     try:
#         response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
#         response.raise_for_status()
#         return response.json()["path"] is not None
#     except Exception as e:
#         print(f"Error checking file path: {e}")
#         return False


# # Test if the specified FILESTORE_PATH exists
# def test_databricks():
#     headers = {"Authorization": f"Bearer {access_token}"}
#     assert check_filestore_path(FILESTORE_PATH, headers) is True


if __name__ == "__main__":
    test_extract()
    test_transform_and_load()
    # test_read_data()
    # test_read_all_data()
    # test_save_data()
    # test_delete_data()
    # test_get_table_columns()
    # test_databricks()
