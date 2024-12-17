"""
Extract data from a url and save as a file
"""

import requests


def extract(url: str, file_name: str, on_databricks=False):
    """ "Extract a url to a file path"""
    file_path = (
        "/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/data/"
        if on_databricks
        else "./data/"
    ) + file_name
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return "Extract Successful"
