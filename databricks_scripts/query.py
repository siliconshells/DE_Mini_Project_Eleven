import sys

sys.path.append("/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/my_lib/")

from lib import execute_read_query
from util import save_output


script_to_execute = """
SELECT 
    indicator.indicator_name, 
    air_quality.time_period, 
    AVG(data_value) AS avg_data_value
FROM 
    air_quality
INNER JOIN 
    indicator 
ON 
    air_quality.fn_indicator_id = indicator.indicator_id
GROUP BY 
    indicator_name, time_period
ORDER BY 
    indicator_name, time_period
"""


def query(on_databricks=False):
    result_df = execute_read_query(script_to_execute)
    result_df.show()
    save_output(result_df.toPandas().to_markdown())
    result_df.write.csv(
        (
            "/Workspace/Workspace/Shared/Leonard_Eshun_Mini_Project_Eleven/data"
            if on_databricks
            else "./Aggregation_Query_Result"
        ),
        header=True,
        mode="overwrite",
    )


if __name__ == "__main__":
    query(True)
