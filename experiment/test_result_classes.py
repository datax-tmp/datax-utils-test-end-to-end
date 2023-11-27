# Databricks notebook source
# MAGIC %md 
# MAGIC #Libraries Import

# COMMAND ----------

# import: pyspark
from pyspark.sql.types import DateType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC # E2E Test Results Class

# COMMAND ----------

class E2ETestResults(AbstractTestResults):
    """
    Class for E2E test's test result, each row of the result is divided by assertion cases.
    """

    def __init__(self, configs):
        """
        Args:
            configs (TestPipelineConfigs): Configuration object containing pipeline settings.
        """
        super().__init__(configs)
        # each row contains one assertion test result
        self.result_schema = StructType(
            [
                StructField("workflow_name", StringType(), True),
                StructField("test_area", StringType(), True),
                StructField("assertions", StringType(), True),
                StructField("log_params", StringType(), True),
                StructField("assertion_result", StringType(), True),
                StructField("workflow_start_time", TimestampType(), True),
                StructField("workflow_end_time", TimestampType(), True),
                StructField("workflow_setup_duration", StringType(), True),
                StructField("workflow_execution_duration", StringType(), True),
                StructField("test_param", StringType(), True),
            ]
        )

# COMMAND ----------

# MAGIC %md
# MAGIC # Performance Test Results

# COMMAND ----------

class PerformanceTestResults(AbstractTestResults):
    """
    Class for E2E test's test result, each row of the result is divided
    by workflow's task and subprocesses.
    """

    def __init__(self, configs):
        """
        Args:
            configs (TestPipelineConfigs): Configuration object containing pipeline settings.
        """
        super().__init__(configs)
        # each row contains one task execution time
        self.result_schema = StructType(
            [
                StructField("workflow_name", StringType(), True),
                StructField("task_name", StringType(), True),
                StructField("sub_process", StringType(), True),
                StructField("log_params", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("setup_duration", StringType(), True),
                StructField("execution_duration", StringType(), True),
                StructField("workflow_start_time", TimestampType(), True),
                StructField("workflow_end_time", TimestampType(), True),
                StructField("workflow_setup_duration", StringType(), True),
                StructField("workflow_execution_duration", StringType(), True),
                StructField("test_param", StringType(), True),
            ]
        )
