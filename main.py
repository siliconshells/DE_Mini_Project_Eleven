from test_main import test_transform_and_load
from my_lib.lib import execute_read_query
from my_lib.util import save_output


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


def main():
    test_transform_and_load()
    result_df = execute_read_query(script_to_execute)
    result_df.show()
    save_output(result_df.toPandas().to_markdown())
    result_df.write.csv("Aggregation_Query_Result", header=True, mode="overwrite")


if __name__ == "__main__":
    main()
