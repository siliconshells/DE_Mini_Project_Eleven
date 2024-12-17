import sys

sys.path.append("/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/my_lib/")

from util import log_tests
from extract_data import extract
import os


def do_extract():
    log_tests("Extraction Test", header=True, new_log_file=True)
    log_tests("Removing existing CSV file exists")
    if os.path.exists(
        "/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/data/air_quality.csv"
    ):
        os.remove(
            "/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/data/air_quality.csv"
        )

    log_tests("Confirming that CSV file doesn't exists...")
    assert not os.path.exists("population_bar.png")
    log_tests("Test Successful")

    log_tests("Extracting data and saving...")
    extract(
        url="https://data.cityofnewyork.us/resource/c3uy-2p5r.csv?$limit=200000",
        file_name="air_quality.csv",
        on_databricks=True,
    )

    log_tests("Testing if CSV file exists...")
    assert os.path.exists(
        "/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/data/air_quality.csv"
    )
    log_tests("Extraction Test Successful", last_in_group=True)
    print("Extraction Test Successful")


if __name__ == "__main__":
    do_extract()
