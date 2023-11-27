# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from abc import ABC
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Abstract Test Results

# COMMAND ----------

class AbstractTestResults(ABC):
    """
    Abstract class for test result classes, used for saving and displaying test results
    """

    test_results = []

    def __init__(self, config: object):
        """
        Args:
            configs (TestPipelineConfigs): Configuration object containing pipeline settings.
        """
        self.config = config
        self.result_schema = StructType(
            [
                StructField("workflow_name", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("execution_time", StringType(), True),
                StructField("test_param", StringType(), True),
            ]
        )

    @classmethod
    def save_results(cls, test_results: list):
        """
        A class method for saving test results to 'test_results'
        Args:
            test_results (list): a list of test results from a test case pipeline
        """
        cls.test_results.extend(test_results)

    def get_test_results_df(self):
        """
        A method for creating the result dataframe from the list of results dict 'test_results'
        Args:
            test_results (list): a list of test results dict from E2E test or performance test
        """
        return spark.createDataFrame(self.test_results, self.result_schema)

    @classmethod
    def clear_class_attribute(cls):
        """
        A method for creating the result dataframe from the list of results dict 'test_results'
        Args:
            test_results (list): a list of test results dict from E2E test or performance test
        """
        cls.test_results = []
