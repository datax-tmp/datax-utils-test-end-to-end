# Databricks notebook source
# MAGIC %md
# MAGIC #Utilities Functions

# COMMAND ----------

import os
os.environ["SPARK_VERSION"] = "3.3"

# COMMAND ----------

# MAGIC %run ../common/test_utils

# COMMAND ----------

# MAGIC %run ../abstract_class/abstract_test_pipeline

# COMMAND ----------

# MAGIC %run ../abstract_class/abstract_test_results

# COMMAND ----------

# MAGIC %run ../abstract_class/abstract_table

# COMMAND ----------

# MAGIC %run ../common/test_result_classes

# COMMAND ----------

# MAGIC %run ../test_area/data_correctness

# COMMAND ----------

# MAGIC %run ../test_area/job_status

# COMMAND ----------

# MAGIC %run ../test_area/workflow_completeness

# COMMAND ----------

# MAGIC %run ../test_area/deequ_correctness

# COMMAND ----------

# MAGIC %run ../test_area/sensor_result

# COMMAND ----------

# MAGIC %run ../test_area/error_handling

# COMMAND ----------

# MAGIC %md 
# MAGIC #Libraries Import

# COMMAND ----------

# import: standard
from datetime import timedelta
from dbldatagen import DateRange

# import: pyspark
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Resources Function

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Test Data Source

# COMMAND ----------

class TableE2ETestDataSource(AbstractTable):
    """
    A class for setting up table for testing with sample workflow
    Naming standard: Table<table_name> in CamelCase.

    Generating 5 days of data: the loading date, last 3 days before loading date,
    and 1 day of last month's end of month data
    """

    def __init__(self, configs):
        """
        Init method of the class TableE2ETestDataSource
        """
        self.data_size = configs.test_param["data_size"]

        # convert dl_data_dt to 'datetime' object, because the 'dbldatagen' library does not support using 'date' as an input
        dl_data_dt = datetime.combine(configs.dl_data_dt, datetime.min.time())

        # there are two cases of the success data source's date
        # if both "input_start_date" and "input_end_date" are "-skip", the data date will be the current date minus 2 days
        # otherwise, use the 'dl_data_dt' from the pipeline's configs
        if (
            configs.workflow_input_param["input_start_date"] != "-skip"
            and configs.workflow_input_param["input_end_date"] != "-skip"
        ):
            mock_data_date = dl_data_dt
        else:
            mock_data_date = datetime.now() - timedelta(days=2)

        # for mocking 4 days of data, mock the start_date by including 3 days of data before the dl_data_dt
        self.start_date = mock_data_date - timedelta(days=3)
        self.end_date = mock_data_date

        # last day of the previous month's date
        self.last_eom_date = mock_data_date.replace(day=1) - timedelta(days=1)

        env = os.environ.get("ENVIRONMENT")
        self.database = f"DATAX{env.upper()}_DATAHUB_FWTEST_DB"
        self.table = "E2E_TEST_DATA_SOURCE"
        self.input_path = f"{configs.data_sources_path}/E2E_TEST_DATA_SOURCE"

    def setup(self) -> None:
        """
        Method to setup table based on configs
        """

        schema = StructType(
            [
                StructField("uc_id", StringType(), True),
                StructField("transactions", IntegerType(), True),
                StructField("dl_data_dt", DateType(), True),
            ]
        )
        # source_df = spark.createDataFrame([], schema)

        # set mock data's config
        # in this mock data, uc_id is a string with length = 12, beginnning with "11", "12", or "13"
        min_uc_id = 10**9
        max_uc_id = (10**10) - 1

        # in this mock data, scb_cust_key is a string with length = 10

        # generating 4 days out of 5 days of data (4/5 multiplied by data size)
        test_data_source_df_spec = (
            dg.DataGenerator(
                spark,
                name="transform_period_source",
                rows=(self.data_size) * (4 / 5),
                partitions=4,
            )
            .withColumn(
                "uc_id",
                StringType(),
                minValue=min_uc_id,
                maxValue=max_uc_id,
                prefix="11",
                textSeparator="",
            )
            .withColumn("transactions", IntegerType(), values=[1])
            .withColumn(
                "dl_data_dt", DateType(), begin=self.start_date, end=self.end_date
            )
        )
        test_data_source_df = test_data_source_df_spec.build()

        if not (self.start_date <= self.last_eom_date <= self.end_date):
            # generating last end of month date data
            test_last_eom_df_spec = (
                dg.DataGenerator(
                    spark,
                    name="transform_period_source_last_eom",
                    rows=(self.data_size) * (1 / 5),
                    partitions=1,
                )
                .withColumn(
                    "uc_id",
                    StringType(),
                    minValue=min_uc_id,
                    maxValue=max_uc_id,
                    prefix="11",
                    textSeparator="",
                )
                .withColumn("transactions", IntegerType(), values=[1])
                .withColumn(
                    "dl_data_dt",
                    DateType(),
                    begin=self.last_eom_date,
                    end=self.last_eom_date,
                )
            )
            test_last_eom_df = test_last_eom_df_spec.build()

            test_data_source_df = test_data_source_df.union(test_last_eom_df)

        test_data_source_df.write.partitionBy("dl_data_dt").mode("overwrite").format(
            "delta"
        ).option("path", self.input_path).option("overwriteSchema", "true").saveAsTable(
            f"{self.database}.{self.table}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup DQ Audit Failure Data Source

# COMMAND ----------

class TableE2ETestDataSourceDQAuditFailure(AbstractTable):
    """
    A class for setting up table for testing with sample workflow
    Naming standard: Table<table_name> in CamelCase.
    """

    def __init__(self, configs):
        """
        Init method of the class TableE2ETestDataSource
        """
        self.data_size = configs.test_param["data_size"]

        # convert dl_data_dt to 'datetime' object, because the 'dbldatagen' library does not support using 'date' as an input
        dl_data_dt = datetime.combine(configs.dl_data_dt, datetime.min.time())

        # for mocking 4 days of data, mock the start_date by including 3 days of data before the dl_data_dt
        self.start_date = dl_data_dt - timedelta(days=3)
        self.end_date = dl_data_dt

        # last day of the previous month's date
        self.last_eom_date = dl_data_dt.replace(day=1) - timedelta(days=1)

        env = os.environ.get("ENVIRONMENT")
        self.database = f"DATAX{env.upper()}_DATAHUB_FWTEST_DB"
        self.table = "E2E_TEST_DATA_SOURCE"
        self.input_path = f"{configs.data_sources_path}/E2E_TEST_DATA_SOURCE"

    def setup(self) -> None:
        """
        Method to setup table based on configs
        """
        # set mock data's config
        # in this mock data, uc_id is a string with length = 12, beginnning with "11", "12", or "13"
        min_uc_id = 10**5
        max_uc_id = (10**10) - 1

        # in this mock data, scb_cust_key is a string with length = 5
        # this will not be conformed with the DQ Audit's rules, forcing a DQ check failure


        # generating 4 days out of 5 days of data (4/5 multiplied by data size)
        test_data_source_df_spec = (
            dg.DataGenerator(
                spark,
                name="transform_period_source",
                rows=(self.data_size) * (4 / 5),
                partitions=4,
            )
            .withColumn(
                "uc_id",
                StringType(),
                minValue=min_uc_id,
                maxValue=max_uc_id,
                prefix="07",
                textSeparator="",
            )
            .withColumn("transactions", IntegerType(), values=[1])
            .withColumn(
                "dl_data_dt", DateType(), begin=self.start_date, end=self.end_date
            )
        )
        test_data_source_df = test_data_source_df_spec.build()

        # generating last end of month date data
        test_last_eom_df_spec = (
            dg.DataGenerator(
                spark,
                name="transform_period_source_last_eom",
                rows=(self.data_size) * (1 / 5),
                partitions=1,
            )
            .withColumn(
                "uc_id",
                StringType(),
                minValue=min_uc_id,
                maxValue=max_uc_id,
                prefix="07",
                textSeparator="",
            )
            .withColumn("transactions", IntegerType(), values=[1])
            .withColumn(
                "dl_data_dt",
                DateType(),
                begin=self.last_eom_date,
                end=self.last_eom_date,
            )
        )
        test_last_eom_df = test_last_eom_df_spec.build()

        union_data_source_df = test_data_source_df.union(test_last_eom_df)

        union_data_source_df.write.partitionBy("dl_data_dt").mode("overwrite").format(
            "delta"
        ).option("path", self.input_path).option("overwriteSchema", "true").saveAsTable(
            f"{self.database}.{self.table}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup DQ Anomaly Failure Data Source

# COMMAND ----------

class TableE2ETestDataSourceDQAnomalyFailure(AbstractTable):
    """
    A class for setting up table for testing with sample workflow
    Naming standard: Table<table_name> in CamelCase.
    """

    def __init__(self, configs):
        """
        Init method of the class TableE2ETestDataSource
        """
        self.data_size = configs.test_param["data_size"]

        # convert dl_data_dt to 'datetime' object, because the 'dbldatagen' library does not support using 'date' as an input
        dl_data_dt = datetime.combine(configs.dl_data_dt, datetime.min.time())

        # for mocking 4 days of data, mock the start_date by including 3 days of data before the dl_data_dt
        self.start_date = dl_data_dt - timedelta(days=3)
        self.end_date = dl_data_dt

        # last day of the previous month's date
        self.last_eom_date = dl_data_dt.replace(day=1) - timedelta(days=1)

        env = os.environ.get("ENVIRONMENT")
        self.database = f"DATAX{env.upper()}_DATAHUB_FWTEST_DB"
        self.table = "E2E_TEST_DATA_SOURCE"
        self.input_path = f"{configs.data_sources_path}/E2E_TEST_DATA_SOURCE"

    def setup(self) -> None:
        """
        Method to setup table based on configs
        """
        # set mock data's config
        # in this mock data, uc_id is a string with length = 12, beginnning with "11", "12", or "13"
        min_uc_id = 10**9
        max_uc_id = (10**10) - 1

        # in this mock data, scb_cust_key is a string with length = 10


        # generating 4 days out of 5 days of data (4/5 multiplied by data size)
        test_data_source_df_spec = (
            dg.DataGenerator(
                spark,
                name="transform_period_source",
                rows=(self.data_size) * (4 / 5),
                partitions=4,
            )
            .withColumn(
                "uc_id",
                StringType(),
                minValue=min_uc_id,
                maxValue=max_uc_id,
                prefix="11",
                textSeparator="",
                uniqueValues=int(self.data_size / 10),
            )
            .withColumn("transactions", IntegerType(), values=[5])
            .withColumn(
                "dl_data_dt", DateType(), begin=self.start_date, end=self.end_date
            )
        )
        test_data_source_df = test_data_source_df_spec.build()

        # generating last end of month date data
        test_last_eom_df_spec = (
            dg.DataGenerator(
                spark,
                name="transform_period_source_last_eom",
                rows=(self.data_size) * (1 / 5),
                partitions=1,
            )
            .withColumn(
                "uc_id",
                StringType(),
                minValue=min_uc_id,
                maxValue=max_uc_id,
                prefix="11",
                textSeparator="",
                uniqueValues=int(self.data_size / 10),
            )
            .withColumn("transactions", IntegerType(), values=[5])
            .withColumn(
                "dl_data_dt",
                DateType(),
                begin=self.last_eom_date,
                end=self.last_eom_date,
            )
        )
        test_last_eom_df = test_last_eom_df_spec.build()

        union_data_source_df = test_data_source_df.union(test_last_eom_df)

        union_data_source_df.write.partitionBy("dl_data_dt").mode("overwrite").format(
            "delta"
        ).option("path", self.input_path).option("overwriteSchema", "true").saveAsTable(
            f"{self.database}.{self.table}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup E2E Test Data Agg Profiler Result

# COMMAND ----------

def setup_e2e_test_data_agg_profiler_result(dq_profiler_path_data_agg: str):
    """
    Function for setting up a mock Deequ Profiling result for 'E2E_TEST_DATA_AGG' table.
    Mock data in JSON format, write to a temp location using Python's 'json' library, and transfer to the Deequ profiler location.

    Args:
        dq_profiler_path_data_agg (str): path for the Deequ profiler

    """
    tmp_profiler_path_data_agg = "tmp_profiler_path_data_agg/"
    mock_analyzer_result = [
        {
            "resultKey": {
                "dataSetDate": 1692796585789,
                "tags": {"date": "2023-08-30", "table_name": "E2E_TEST_DATA_AGG"},
            },
            "analyzerContext": {
                "metricMap": [
                    {
                        "analyzer": {
                            "analyzerName": "Maximum",
                            "column": "class_source_product_sum_transactions_val_dly_v1",
                        },
                        "metric": {
                            "metricName": "DoubleMetric",
                            "entity": "Column",
                            "instance": "class_source_product_sum_transactions_val_dly_v1",
                            "name": "Maximum",
                            "value": 1.0,
                        },
                    },
                    {
                        "analyzer": {
                            "analyzerName": "Minimum",
                            "column": "class_source_product_sum_transactions_val_dly_v1",
                        },
                        "metric": {
                            "metricName": "DoubleMetric",
                            "entity": "Column",
                            "instance": "class_source_product_sum_transactions_val_dly_v1",
                            "name": "Minimum",
                            "value": 1.0,
                        },
                    },
                ]
            },
        }
    ]
    # create the mocking result file's directory
    dbutils.fs.mkdirs(tmp_profiler_path_data_agg)

    # write the mock json file to DBFS
    with open(f"/dbfs/{tmp_profiler_path_data_agg}result.json", "w+") as json_file:
        json.dump(mock_analyzer_result, json_file, indent=4)

    # move the mocked json file to ADLS
    dbutils.fs.mv(f"{tmp_profiler_path_data_agg}result.json", dq_profiler_path_data_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC #Test Area Class

# COMMAND ----------

# import from common, any specific test area will be added here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path Definitions

# COMMAND ----------

# set up path parameters
env = os.environ.get("ENVIRONMENT")
storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT")

if env.upper() == "SIT" or env.upper() == "NPR":
    deequ_check_dir = "/deequ-check"
    deequ_analyzer_engine_dir = "/deequ_analyzer"
    fw_sensor_dir = "/fw-sensor"
    input_dir = ""
    output_dir = ""
else:
    deequ_check_dir = f"abfss://deequ-check@{storage_account}"
    deequ_analyzer_engine_dir = f"abfss://deequ-analyzer@{storage_account}"
    fw_sensor_dir = f"abfss://fw-sensor@{storage_account}"
    input_dir = f"abfss://core@{storage_account}"
    output_dir = f"abfss://core@{storage_account}"

# setup's inputs
data_sources_path = f"{input_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB"
e2e_test_data_source_path = f"{data_sources_path}/E2E_TEST_DATA_SOURCE/"

# workflow's outputs
dq_audit_path_data_agg = f"{deequ_check_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/result.json"
dq_audit_path_data_output = f"{deequ_check_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_OUTPUT/result.json"
dq_profiler_path_data_agg = f"{deequ_analyzer_engine_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/result.json"
dq_profiler_all_metrics_result_path = f"{deequ_analyzer_engine_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/e2e_data_profiling/result_with_problematic_metrics.json"
dq_profiler_without_problematic_metrics_result_path = f"{deequ_analyzer_engine_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/e2e_data_profiling/result_without_problematic_metrics.json"

dq_anomaly_result_data_agg = (
    f"/deequ-anomaly/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/result.json"
)
dq_anomaly_result_data_output = f"/deequ-anomaly/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_OUTPUT/result.json"
e2e_test_data_agg_path = (
    f"{output_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/"
)
e2e_test_data_output_path = (
    f"{output_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_OUTPUT/"
)

# engine checkpoints
profiler_engine_checkpoint_data_agg = f"{deequ_analyzer_engine_dir}/checkpoint/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/"
sensor_fw_checkpoint_data_source_d3 = f"{fw_sensor_dir}/checkpoint/UpstreamPipeline/TransformFixedDatePipeline/e2e_test_data_source_anomaly_d3"
sensor_fw_checkpoint_data_source_m1 = f"{fw_sensor_dir}/checkpoint/UpstreamPipeline/TransformFixedDatePipeline/e2e_test_data_source_anomaly_m1"
sensor_fw_checkpoint_data_agg_d1 = f"{fw_sensor_dir}/checkpoint/TransformFixedDatePipeline/TransformStreamingPipeline/e2e_test_data_agg_anomaly_d1"

# COMMAND ----------

teardown_paths = [
    # setup's inputs
    e2e_test_data_source_path,
    # workflow's outputs
    dq_audit_path_data_agg,
    dq_audit_path_data_output,
    dq_profiler_path_data_agg,
    dq_anomaly_result_data_agg,
    dq_anomaly_result_data_output,
    e2e_test_data_agg_path,
    e2e_test_data_output_path,
    dq_profiler_all_metrics_result_path,
    dq_profiler_without_problematic_metrics_result_path,
    # engine checkpoints
    profiler_engine_checkpoint_data_agg,
    sensor_fw_checkpoint_data_source_d3,
    sensor_fw_checkpoint_data_source_m1,
    sensor_fw_checkpoint_data_agg_d1,
]

# COMMAND ----------

# MAGIC %md
# MAGIC # E2E Test Init Task

# COMMAND ----------

# MAGIC %md
# MAGIC This test case is being used to test the task 'init_task', which is the first task in every workflow, <br>
# MAGIC if it correctly sets the 'start_date' and 'end_date' correctly if 'input_start_date' and 'input_end_date' are set to '-skip' <br>
# MAGIC **Expectation**: the data date set by 'init_task' should be -2 days from the current running date. <br>
# MAGIC **Related test ares**:
# MAGIC - Data Correctness
# MAGIC   - check_output_table_partitions
# MAGIC   - check_table_schema (E2E_TEST_DATA_AGG)
# MAGIC - Job Status
# MAGIC   - check_log_period_loading (E2E_TEST_DATA_AGG)
# MAGIC   - check_log_run_detail (TransformFixedDatePipeline)
# MAGIC - Deequ Correctness
# MAGIC   - check_deequ_array_column (E2E_TEST_DATA_AGG's DQ Audit results)
# MAGIC   
# MAGIC **Setup Resources**:
# MAGIC - Table: "DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_SOURCE" <br>
# MAGIC Mocking data with end_date being current running date minus 2 days and start_date being the end_date - 3 days (sum into 4 days of data)
# MAGIC then union with one day of data being the end of previous month only if the data from start_date to end_date does not include the end of previous month data, counting from the end_date.
# MAGIC Each day of data contains the same row count.<br>
# MAGIC
# MAGIC
# MAGIC - E2E_DATA_AGG's profiler result <br>
# MAGIC Path: "{deequ_analyzer_engine_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/result.json" <br>
# MAGIC Served as the historical data for E2E_DATA_AGG (output from the task 'TransformFixedDatePipeline'), to be used to check in anomaly task 'TransformStreamingDeequAnomalyPipeline'.

# COMMAND ----------

class E2ETestInitTaskPipeline(AbstractTestPipeline):
    """
    Represents an end-to-end test pipeline for validating test area. This pipeline sets up resources, runs tests, saves results,
    and then tears down resources after execution.
    """

    def __init__(self, configs) -> None:
        """
        Args:
            configs (TestPipelineConfigs): Configuration object containing pipeline settings.
        """
        super().__init__(configs)

    def setup_resource(self) -> None:
        """
        Set up necessary resources, including databases and tables, for test execution.
        """
        # Setup database (from utils)
        setup_database(self.configs.input_data_endpoint.split(".")[0])

        # Setup table for each table class in the config
        for table_class in self.configs.table_classes:
            table_class_obj = table_class(configs=self.configs)
            table_class_obj.setup()

        # Setup table for historical DQ Profiler result, to be used with DQ Anomaly
        setup_e2e_test_data_agg_profiler_result(dq_profiler_path_data_agg)

    def run_test(self, *args, **kwargs) -> list:
        """
        Execute a series of test cases. Returns a list of test assertion results.
        The assertion results are updated in the class attribute "test_results"
        """
        assert_results = []
        env = os.environ.get("ENVIRONMENT").upper()

        batch_output_table = f"DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_AGG"
        stream_output_table = (
            f"DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_OUTPUT"
        )
        log_endpoint = f"DATAX{env}_DATAHUB_FW_LOG_DB.TBL_ETL_DATA_STS_LOG"
        sensor_log_endpoint = f"DATAX{env}_DATAHUB_FW_SNSR_DB.TBL_SNSR_RSLT"

        expected_output_date = (datetime.now() - timedelta(days=2)).date()

        ###################################### Batch Section #########################################
        # DataCorrectness
        assert_results.append(
            DataCorrectness.check_output_table_partitions(
                target_table=batch_output_table, 
                expected_values=[expected_output_date], 
                log_params=["target_table", "expected_values"],
            )
        )

        assert_results.append(
            DataCorrectness.check_table_schema(
                database_name=f"DATAX{env.upper()}_DATAHUB_FWTEST_DB",
                table_name="E2E_TEST_DATA_AGG",
                expected_schema={
                    "uc_id": StringType(),
                    "class_source_product_sum_transactions_val_dly_v1": IntegerType(),
                    "dl_data_dt": DateType(),
                    "dl_load_ts": TimestampType(),
                },
                log_params=["database_name", "table_name"],
            )
        )

        # JobStatus
        assert_results.append(
            JobLogCorrectness.check_log_period_loading(
                task_name="TransformFixedDatePipeline",
                target_table=batch_output_table,
                workflow_run_id=self.workflow_run_id,
                log_endpoint=log_endpoint,
                log_params=["target_table"],
            )
        )

        assert_results.append(
            JobLogCorrectness.check_log_run_detail(
                task_name="TransformFixedDatePipeline",
                workflow_run_id=self.workflow_run_id,
                log_endpoint=log_endpoint,
                expected_count=1,
                log_params=["task_name"],
            )
        )

        ###################################### Deequ Section #########################################

        # audit dataxsit_datahub_fwtest_db.e2e_test_data_agg
        expected_audit_date_e2e_test_data_agg = datetime.strftime(
            expected_output_date, "%Y-%m-%d"
        )
        assert_results.append(
            DeequCorrectness.check_deequ_array_column(
                path=dq_audit_path_data_agg,
                expected_values=[expected_audit_date_e2e_test_data_agg],
                column_name="dt_list",
                log_params=["path"],
            )
        )

        # Test results might be a list of dict, to be created into the result dataframe
        super().update_test_results(
            assert_results, self.configs.test_param, skip_workflow_info_flag=False
        )

    def save_results(self, *args, **kwargs) -> None:
        """
        Save the test results to the specified class object.
        """
        self.configs.result_class.save_results(self.test_results)

    def teardown_resource(self):
        """
        Clean up resources created during setup, including databases, tables, and output files.
        """
        # Teardown each setup table
        for table_class in self.configs.table_classes:
            table_class_obj = table_class(configs=self.configs)
            table_class_obj.teardown()

        # Teardown output table
        for table in self.configs.output_table:
            teardown_table(self.configs.output_database, table)

        # Teardown file system's output
        for path in self.configs.teardown_paths:
            dbutils.fs.rm(path, recurse=True)

    def execute(self) -> None:
        """
        Orchestrates the entire test pipeline by sequentially calling setup_resource(),
        trigger_workflow(), run_test(), save_results(), and teardown_resource() methods.
        """
        self.teardown_resource()
        self.setup_resource()
        self.trigger_workflow(validate_task_success=False)
        self.run_test()
        self.save_results()
        self.teardown_resource()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Tests

# COMMAND ----------

test_init_task_configs = {
    # required parameters
    "workflow_job_id": 685909697359564,
    "pipeline_class": E2ETestInitTaskPipeline,
    "table_classes": [TableE2ETestDataSource],
    "result_class": E2ETestResults,
    "dl_data_dt": date(2023, 8, 31),
    # optional parameters
    "workflow_input_param": {
        "input_start_date": "-skip",
        "input_end_date": "-skip",
        "TransformFixedDateDeequProfilerPipeline": "bypass",
        "TransformFixedDateTechnicalViewGenerator": "bypass",
        "TransformFixedDateBusinessViewGenerator": "bypass",
        "TransformStreamingDeequAnomalyPipeline": "bypass",
        "TransformStreamingPipeline": "bypass",
    },
    "input_data_endpoint": f"DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_SOURCE",
    "data_sources_path": data_sources_path,
    "output_database": f"DATAX{env.upper()}_DATAHUB_FWTEST_DB",
    "output_table": ["E2E_TEST_DATA_AGG", "E2E_TEST_DATA_OUTPUT"],
    "teardown_paths": teardown_paths,
    "test_param": {"data_size": [100]},
}

test_init_task = TestTask(test_init_task_configs)
test_init_task.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # E2E Test Pipeline Success

# COMMAND ----------

# MAGIC %md
# MAGIC This test case mocks a transformation workflow that should be success. The pipeline includes the following: <br>
# MAGIC - init_task
# MAGIC - TransformFixedDateDeequAnomalyPipelineD3 - checking anomaly from the past 3 days of data
# MAGIC - TransformFixedDateDeequAnomalyPipelineM1 - checking anomaly from last month data
# MAGIC - TransformFixedDatePipeline - transform using the input start_date and end_date
# MAGIC - TransformFixedDateDeequProfilerPipeline - profiling on the output of the TransformFixedDatePipeline
# MAGIC - TransformStreamingDeequAnomalyPipeline - checking anomaly on the output of the TransformFixedDatePipeline
# MAGIC - TransformStreamingPipeline - transform using Spark's readStream
# MAGIC - TransformFixedDateTechnicalViewGenerator - Technical view generator
# MAGIC - TransformFixedDateBusinessViewGenerator - Business view generator <br>
# MAGIC
# MAGIC **Expectation**: all the tasks runs should be success and produce the correct result, tested by assertions <br>
# MAGIC **Related test ares**:
# MAGIC - Workflow Completeness
# MAGIC   - check_all_tasks_complete
# MAGIC - Data Correctness
# MAGIC   - check_output_table_partitions
# MAGIC   - check_table_schema (E2E_TEST_DATA_AGG, E2E_TEST_DATA_OUTPUT)
# MAGIC - Job Status
# MAGIC   - check_log_period_loading (E2E_TEST_DATA_AGG, E2E_TEST_DATA_OUTPUT)
# MAGIC   - check_log_run_detail (TransformFixedDatePipeline, TransformStreamingPipeline)
# MAGIC   - check_log_cstm_val (E2E_TEST_DATA_AGG)
# MAGIC - Deequ Correctness
# MAGIC   - check_deequ_array_column (E2E_TEST_DATA_AGG and E2E_TEST_DATA_OUTPUT's DQ Audit results)
# MAGIC   - check_deequ_string_column (E2E_TEST_DATA_AGG's profiler result)
# MAGIC   - check_deequ_profiling_results_match_expected (E2E_TEST_DATA_AGG) for the following two cases:
# MAGIC     1. Run Deequ profiling with all metrics - expect negative (disappearance of some metrics).
# MAGIC     2. Run Deequ profiling with all metrics without the problematic metrics - expect positive.
# MAGIC - Sensor Result
# MAGIC   - check_deequ_sensor_result_table (TransformFixedDatePipeline as upstream pipeline) 
# MAGIC
# MAGIC **Setup Resources**:
# MAGIC - Table: "DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_SOURCE" <br>
# MAGIC Mocking data with end_date according to 'dl_data_dt' and start_date being 3 days prior (for a total of 4 days), 
# MAGIC and mock end of last month data if it is not between start_date and end_date, then union them for the total of 4 or 5 days of data.
# MAGIC Each day of data contains the same row count.
# MAGIC
# MAGIC
# MAGIC - E2E_DATA_AGG's profiler result <br>
# MAGIC Path: "{deequ_analyzer_engine_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/result.json" <br>
# MAGIC Served as the historical data for E2E_DATA_AGG (output from the task 'TransformFixedDatePipeline'), to be used to check in anomaly task 'TransformStreamingDeequAnomalyPipeline'.

# COMMAND ----------

class E2ETestSuccessPipeline(AbstractTestPipeline):
    """
    Represents an end-to-end test pipeline for validating test area. This pipeline sets up resources, runs tests, saves results,
    and then tears down resources after execution.
    """

    def __init__(self, configs) -> None:
        """
        Args:
            configs (TestPipelineConfigs): Configuration object containing pipeline settings.
        """
        super().__init__(configs)

    def setup_resource(self) -> None:
        """
        Set up necessary resources, including databases and tables, for test execution.
        """
        # Setup database (from utils)
        setup_database(self.configs.input_data_endpoint.split(".")[0])

        # Setup table for each table class in the config
        for table_class in self.configs.table_classes:
            table_class_obj = table_class(configs=self.configs)
            table_class_obj.setup()

        # Setup table for historical DQ Profiler result, to be used with DQ Anomaly
        setup_e2e_test_data_agg_profiler_result(self.configs.dq_profiler_path_data_agg)

    def run_test(self, *args, **kwargs) -> list:
        """
        Execute a series of test cases. Returns a list of test assertion results.
        The assertion results are updated in the class attribute "test_results"
        """
        assert_results = []
        env = os.environ.get("ENVIRONMENT").upper()

        batch_output_table = f"DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_AGG"
        stream_output_table = (
            f"DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_OUTPUT"
        )
        log_endpoint = f"DATAX{env}_DATAHUB_FW_LOG_DB.TBL_ETL_DATA_STS_LOG"
        sensor_log_endpoint = f"DATAX{env}_DATAHUB_FW_SNSR_DB.TBL_SNSR_RSLT"

        # Completeness area
        assert_results.append(
            WorkflowCompleteness.check_all_tasks_complete(
                workflow_run_id=self.workflow_run_id, 
                log_params=["workflow_run_id"],
            )
        )

        ###################################### Batch Section #########################################
        # DataCorrectness
        assert_results.append(
            DataCorrectness.check_output_table_partitions(
                target_table=batch_output_table, expected_values=[date(2023, 8, 31)]
            )
        )

        assert_results.append(
            DataCorrectness.check_table_schema(
                database_name=f"DATAX{env.upper()}_DATAHUB_FWTEST_DB",
                table_name="E2E_TEST_DATA_AGG",
                expected_schema={
                    "uc_id": StringType(),
                    "class_source_product_sum_transactions_val_dly_v1": IntegerType(),
                    "dl_data_dt": DateType(),
                    "dl_load_ts": TimestampType(),
                },
                log_params=["database_name", "table_name"],
            )
        )

        # JobStatus
        assert_results.append(
            JobLogCorrectness.check_log_period_loading(
                task_name="TransformFixedDatePipeline",
                target_table=batch_output_table,
                workflow_run_id=self.workflow_run_id,
                log_endpoint=log_endpoint,
                log_params=["task_name", "target_table", "workflow_run_id", "log_endpoint"],
            )
        )

        assert_results.append(
            JobLogCorrectness.check_log_run_detail(
                task_name="TransformFixedDatePipeline",
                workflow_run_id=self.workflow_run_id,
                log_endpoint=log_endpoint,
                expected_count=1,
                log_params=["task_name", "workflow_run_id", "log_endpoint", "expected_count"],
            )
        )
        ###################################### Streaming Section #########################################
        # DataCorrectness
        assert_results.append(
            DataCorrectness.check_table_schema(
                database_name=f"DATAX{env.upper()}_DATAHUB_FWTEST_DB",
                table_name="E2E_TEST_DATA_OUTPUT",
                expected_schema={
                    "uc_id": StringType(),
                    "class_source_product_sum_transactions_val_dly_v1": IntegerType(),
                    "class_source_product_added_transactions_val_dly_v1": IntegerType(),
                    "dl_data_dt": DateType(),
                    "dl_load_ts": TimestampType(),
                },
                log_params=["database_name", "table_name"],
            )
        )

        # DataLoading
        assert_results.append(
            DataCorrectness.check_output_table_partitions(
                target_table=stream_output_table, 
                expected_values=[date(2023, 8, 31)],
                log_params=["target_table", "expected_values"],
            )
        )

        assert_results.append(
            DataCorrectness.check_date_period_loading(
                target_table=stream_output_table,
                start_date=date(2023, 8, 31),
                end_date=date(2023, 8, 31),
                log_params=["target_table", "start_date", "end_date"],
            )
        )

        # JobStatus
        assert_results.append(
            JobLogCorrectness.check_log_period_loading(
                task_name="TransformStreamingPipeline",
                target_table=stream_output_table,
                workflow_run_id=self.workflow_run_id,
                log_endpoint=log_endpoint,
                log_params=["task_name", "target_table", "workflow_run_id", "log_endpoint"],
            )
        )

        assert_results.append(
            JobLogCorrectness.check_log_run_detail(
                task_name="TransformStreamingPipeline",
                workflow_run_id=self.workflow_run_id,
                log_endpoint=log_endpoint,
                expected_count=1,
                log_params=["task_name", "workflow_run_id", "log_endpoint", "expected_count"],
            )
        )

        ###################################### Deequ Section #########################################

        # audit dataxsit_datahub_fwtest_db.e2e_test_data_agg
        assert_results.append(
            DeequCorrectness.check_deequ_array_column(
                path=dq_audit_path_data_agg,
                expected_values=["2023-08-31"],
                column_name="dt_list",
                log_params=["path", "expected_values", "column_name"],
            )
        )

        # audit dataxsit_datahub_fwtest_db.e2e_test_data_output
        assert_results.append(
            DeequCorrectness.check_deequ_array_column(
                path=dq_audit_path_data_output,
                expected_values=["2023-08-31"],
                column_name="dt_list",
                log_params=["path", "expected_values", "column_name"],
            )
        )

        # audit dataxsit_datahub_fwtest_db.e2e_test_data_agg warning log
        assert_results.append(
            JobLogCorrectness.check_log_cstm_val(
                task_name="TransformFixedDatePipeline",
                workflow_run_id=self.workflow_run_id,
                log_endpoint=log_endpoint,
                check_key_word="No such struct field val in key, value; line 1 pos 0",
                log_params=["task_name", "workflow_run_id", "log_endpoint", "check_key_word"],
            )
        )

        # profiler date dataxsit_datahub_fwtest_db.e2e_test_data_agg
        assert_results.append(
            DeequCorrectness.check_deequ_string_column(
                path=dq_profiler_path_data_agg,
                expected_values=["2023-08-31"],
                column_name="date",
                log_params=["path", "expected_values", "column_name"],
            )
        )

        # anomaly dataxsit_datahub_fwtest_db.e2e_test_data_source
        assert_results.append(
            SensorResult.check_deequ_sensor_result_table(
                workflow_run_id=self.workflow_run_id,
                upstream_pipeline="TransformFixedDatePipeline",
                expected_values=["2023-08-31"],
                column_name="dt",
                log_params=["workflow_run_id", "upstream_pipeline", "expected_values", "column_name"],
            )
        )

        ################################## Deequ Profile Results #####################################
        ######################## checking result completeness of test case 1: ########################
        # profiler result of dataxsit_datahub_fwtest_db.e2e_test_data_agg
        # assert result for profiling `uc_id` column
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_all_metrics_result_path,
                data_date="2023-08-31",
                data_column="uc_id",
                expected_config_info={'Histogram.ratio.111000000071', 'Histogram.abs.111000000071', 'Histogram.ratio.111000000063', 'Histogram.ratio.111000000043', 'Histogram.ratio.111000000067', 'Histogram.abs.111000000059', 'Histogram.abs.111000000047', 'Histogram.ratio.111000000031', 'Histogram.ratio.111000000051', 'Histogram.ratio.111000000007', 'Histogram.abs.111000000079', 'Histogram.abs.111000000035', 'Histogram.abs.111000000063', 'Histogram.abs.111000000003', 'Histogram.ratio.111000000075', 'Histogram.abs.111000000019', 'Histogram.abs.111000000075', 'Histogram.abs.111000000067', 'Histogram.abs.111000000023', 'Histogram.ratio.111000000003', 'Histogram.bins', 'Histogram.ratio.111000000015', 'Histogram.ratio.111000000055', 'Histogram.ratio.111000000079', 'Histogram.abs.111000000051', 'Histogram.abs.111000000031', 'Histogram.abs.111000000055', 'Histogram.abs.111000000027', 'Histogram.ratio.111000000023', 'Histogram.ratio.111000000059', 'Histogram.ratio.111000000035', 'Histogram.ratio.111000000019', 'Histogram.abs.111000000007', 'Histogram.ratio.111000000011', 'Histogram.ratio.111000000039', 'Histogram.ratio.111000000027', 'Histogram.abs.111000000015', 'Histogram.ratio.111000000047', 'Histogram.abs.111000000011', 'Histogram.abs.111000000039', 'Histogram.abs.111000000043'},
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )

        # assert result for profiling `class_source_product_sum_transactions_val_dly_v1` column 
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_all_metrics_result_path,
                data_date="2023-08-31",
                data_column="class_source_product_sum_transactions_val_dly_v1",
                expected_config_info={"Distinctness"},
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )

        # assert result for profiling `uc_id` column 
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_all_metrics_result_path,
                data_date="2023-08-31",
                data_column="uc_id",
                expected_config_info={'Histogram.ratio.111000000071', 'Histogram.abs.111000000071', 'Histogram.ratio.111000000063', 'Histogram.ratio.111000000043', 'Histogram.ratio.111000000067', 'Histogram.abs.111000000059', 'Histogram.abs.111000000047', 'Histogram.ratio.111000000031', 'Histogram.ratio.111000000051', 'Histogram.ratio.111000000007', 'Histogram.abs.111000000079', 'Histogram.abs.111000000035', 'Histogram.abs.111000000063', 'Histogram.abs.111000000003', 'Histogram.ratio.111000000075', 'Histogram.abs.111000000019', 'Histogram.abs.111000000075', 'Histogram.abs.111000000067', 'Histogram.abs.111000000023', 'Histogram.ratio.111000000003', 'Histogram.bins', 'Histogram.ratio.111000000015', 'Histogram.ratio.111000000055', 'Histogram.ratio.111000000079', 'Histogram.abs.111000000051', 'Histogram.abs.111000000031', 'Histogram.abs.111000000055', 'Histogram.abs.111000000027', 'Histogram.ratio.111000000023', 'Histogram.ratio.111000000059', 'Histogram.ratio.111000000035', 'Histogram.ratio.111000000019', 'Histogram.abs.111000000007', 'Histogram.ratio.111000000011', 'Histogram.ratio.111000000039', 'Histogram.ratio.111000000027', 'Histogram.abs.111000000015', 'Histogram.ratio.111000000047', 'Histogram.abs.111000000011', 'Histogram.abs.111000000039', 'Histogram.abs.111000000043'},
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )

        # assert result for Correlation metric
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_all_metrics_result_path,
                data_date="2023-08-31",
                data_column="class_source_product_sum_transactions_val_dly_v1,class_source_product_sum_transactions_val_dly_v1",
                expected_config_info=set(),
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )

        # assert result for MutualInformation metric
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_all_metrics_result_path,
                data_date="2023-08-31",
                data_column="class_source_product_sum_transactions_val_dly_v1,uc_id",
                expected_config_info=set(),
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )

        # assert result for Size metric
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_all_metrics_result_path,
                data_date="2023-08-31",
                data_column="*",
                expected_config_info=set(),
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )
        ######################## checking result completeness of test case 2: ########################
        # profiler result of dataxsit_datahub_fwtest_db.e2e_test_data_agg
        # assert result for profiling `uc_id` column
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_without_problematic_metrics_result_path,
                data_date="2023-08-31",
                data_column="uc_id",
                expected_config_info={'Histogram.ratio.111000000071', 'Histogram.abs.111000000071', 'Histogram.ratio.111000000063', 'Histogram.ratio.111000000043', 'Histogram.ratio.111000000067', 'Histogram.abs.111000000059', 'Histogram.abs.111000000047', 'Histogram.ratio.111000000031', 'Histogram.ratio.111000000051', 'Histogram.ratio.111000000007', 'Histogram.abs.111000000079', 'Histogram.abs.111000000035', 'Histogram.abs.111000000063', 'Histogram.abs.111000000003', 'Histogram.ratio.111000000075', 'UniqueValueRatio', 'Histogram.abs.111000000019', 'Histogram.abs.111000000075', 'Histogram.abs.111000000067', 'Histogram.abs.111000000023', 'Histogram.ratio.111000000003', 'CountDistinct', 'Histogram.bins', 'Histogram.ratio.111000000015', 'Histogram.ratio.111000000055', 'Histogram.ratio.111000000079', 'Histogram.abs.111000000051', 'Histogram.abs.111000000031', 'Histogram.abs.111000000055', 'Histogram.abs.111000000027', 'Histogram.ratio.111000000023', 'Histogram.ratio.111000000059', 'Histogram.ratio.111000000035', 'Histogram.ratio.111000000019', 'Histogram.abs.111000000007', 'Histogram.ratio.111000000011', 'Histogram.ratio.111000000039', 'Histogram.ratio.111000000027', 'Histogram.abs.111000000015', 'Histogram.ratio.111000000047', 'Histogram.abs.111000000011', 'Histogram.abs.111000000039', 'Histogram.abs.111000000043', 'Uniqueness'},
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )

        # assert result for profiling `class_source_product_sum_transactions_val_dly_v1` column 
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_without_problematic_metrics_result_path,
                data_date="2023-08-31",
                data_column="class_source_product_sum_transactions_val_dly_v1",
                expected_config_info={"Distinctness"},
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )

        # assert result for profiling `uc_id` column 
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_without_problematic_metrics_result_path,
                data_date="2023-08-31",
                data_column="uc_id",
                expected_config_info={'Histogram.ratio.111000000071', 'Histogram.abs.111000000071', 'Histogram.ratio.111000000063', 'Histogram.ratio.111000000043', 'Histogram.ratio.111000000067', 'Histogram.abs.111000000059', 'Histogram.abs.111000000047', 'Histogram.ratio.111000000031', 'Histogram.ratio.111000000051', 'Histogram.ratio.111000000007', 'Histogram.abs.111000000079', 'Histogram.abs.111000000035', 'Histogram.abs.111000000063', 'Histogram.abs.111000000003', 'Histogram.ratio.111000000075', 'UniqueValueRatio', 'Histogram.abs.111000000019', 'Histogram.abs.111000000075', 'Histogram.abs.111000000067', 'Histogram.abs.111000000023', 'Histogram.ratio.111000000003', 'CountDistinct', 'Histogram.bins', 'Histogram.ratio.111000000015', 'Histogram.ratio.111000000055', 'Histogram.ratio.111000000079', 'Histogram.abs.111000000051', 'Histogram.abs.111000000031', 'Histogram.abs.111000000055', 'Histogram.abs.111000000027', 'Histogram.ratio.111000000023', 'Histogram.ratio.111000000059', 'Histogram.ratio.111000000035', 'Histogram.ratio.111000000019', 'Histogram.abs.111000000007', 'Histogram.ratio.111000000011', 'Histogram.ratio.111000000039', 'Histogram.ratio.111000000027', 'Histogram.abs.111000000015', 'Histogram.ratio.111000000047', 'Histogram.abs.111000000011', 'Histogram.abs.111000000039', 'Histogram.abs.111000000043', 'Uniqueness'},
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )

        # assert result for Correlation metric
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_without_problematic_metrics_result_path,
                data_date="2023-08-31",
                data_column="class_source_product_sum_transactions_val_dly_v1,class_source_product_sum_transactions_val_dly_v1",
                expected_config_info=set(),
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )

        # assert result for Size metric
        assert_results.append(
            DeequCorrectness.check_deequ_profiling_results_match_expected(
                path=dq_profiler_without_problematic_metrics_result_path,
                data_date="2023-08-31",
                data_column="*",
                expected_config_info=set(),
                log_params=["path", "data_date", "data_column", "expected_config_info"],
            )
        )

        # Test results might be a list of dict, to be created into the result dataframe
        super().update_test_results(
            assert_results, self.configs.test_param, skip_workflow_info_flag=False
        )

    def save_results(self, *args, **kwargs) -> None:
        """
        Save the test results to the specified class object.
        """
        self.configs.result_class.save_results(self.test_results)

    def teardown_resource(self):
        """
        Clean up resources created during setup, including databases, tables, and output files.
        """
        # Teardown each setup table
        for table_class in self.configs.table_classes:
            table_class_obj = table_class(configs=self.configs)
            table_class_obj.teardown()

        # Teardown output table
        for table in self.configs.output_table:
            teardown_table(self.configs.output_database, table)

        # Teardown file system's output
        for path in self.configs.teardown_paths:
            dbutils.fs.rm(path, recurse=True)

    def execute(self) -> None:
        """
        Orchestrates the entire test pipeline by sequentially calling setup_resource(),
        trigger_workflow(), run_test(), save_results(), and teardown_resource() methods.
        """
        self.teardown_resource()
        self.setup_resource()
        self.trigger_workflow(validate_task_success=False)
        self.run_test()
        self.save_results()
        self.teardown_resource()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Tests

# COMMAND ----------

test_success_task_configs = {
    # required parameters
    "workflow_job_id": 685909697359564,
    "pipeline_class": E2ETestSuccessPipeline,
    "table_classes": [TableE2ETestDataSource],
    "result_class": E2ETestResults,
    "dl_data_dt": date(2023, 8, 31),
    # optional parameters
    "workflow_input_param": {
        "input_start_date": "2023-08-31",
        "input_end_date": "2023-08-31",
    },
    "input_data_endpoint": f"DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_SOURCE",
    "data_sources_path": data_sources_path,
    "dq_profiler_path_data_agg": dq_profiler_path_data_agg,
    "output_database": f"DATAX{env.upper()}_DATAHUB_FWTEST_DB",
    "output_table": ["E2E_TEST_DATA_AGG", "E2E_TEST_DATA_OUTPUT"],
    "teardown_paths": teardown_paths,
    "test_param": {"data_size": [100]},
}

test_success_task = TestTask(test_success_task_configs)
test_success_task.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # E2E Test Pipeline DQ Audit Failure

# COMMAND ----------

# MAGIC %md
# MAGIC This test case mocks a data with 'scb_cust_key' that does not conform to the rule set in DQ Audit config<br>
# MAGIC The workflow is expected to fail at the task 'TransformFixedDatePipeline' without writing the data.
# MAGIC
# MAGIC **Expectation**: The task 'TransformFixedDatePipeline' should fail, downstream tasks should not be run, data output should not be written, <br>
# MAGIC and the etl job status log table should log the error correctly.<br>
# MAGIC **Related test ares**:
# MAGIC - Error Handling 
# MAGIC   - assert_error_downstream_task_run
# MAGIC   - check_table_not_exist
# MAGIC   - check_log_failed_detail
# MAGIC
# MAGIC **Setup Resources**:
# MAGIC - Table: "DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_SOURCE" <br>
# MAGIC Mocking data with end_date according to 'dl_data_dt' and start_date being 3 days prior (for a total of 4 days), 
# MAGIC and mock end of last month data if it is not between start_date and end_date, then union them for the total of 4 or 5 days of data.
# MAGIC Each day of data contains the same row count. <br>
# MAGIC In this case, the column 'scb_cust_key' is intentionally mock to contain only 5 digits, which does not conform with the DQ Audit rule and will cause an error.
# MAGIC
# MAGIC
# MAGIC - E2E_DATA_AGG's profiler result <br>
# MAGIC Path: "{deequ_analyzer_engine_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/result.json" <br>
# MAGIC Served as the historical data for E2E_DATA_AGG (output from the task 'TransformFixedDatePipeline'), to be used to check in anomaly task 'TransformStreamingDeequAnomalyPipeline'.

# COMMAND ----------

class E2ETestPipelineDQAuditFailure(AbstractTestPipeline):
    """
    Represents an end-to-end test pipeline for validating test area. This pipeline sets up resources, runs tests, saves results,
    and then tears down resources after execution.
    """

    def __init__(self, configs) -> None:
        """
        Args:
            configs (TestPipelineConfigs): Configuration object containing pipeline settings.
        """
        super().__init__(configs)

    def setup_resource(self) -> None:
        """
        Set up necessary resources, including databases and tables, for test execution.
        """
        # Setup database (from utils)
        setup_database(self.configs.input_data_endpoint.split(".")[0])

        # Setup table for each table class in the config
        for table_class in self.configs.table_classes:
            table_class_obj = table_class(configs=self.configs)
            table_class_obj.setup()

        # Setup table for historical DQ Profiler result, to be used with DQ Anomaly
        setup_e2e_test_data_agg_profiler_result(dq_profiler_path_data_agg)

    def run_test(self, *args, **kwargs) -> list:
        """
        Execute a series of test cases. Returns a list of test assertion results.
        The assertion results are updated in the class attribute "test_results"
        """
        assert_results = []
        env = os.environ.get("ENVIRONMENT").upper()
        batch_output_table = f"DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_AGG"

        log_endpoint = f"DATAX{env}_DATAHUB_FW_LOG_DB.TBL_ETL_DATA_STS_LOG"
        sensor_log_endpoint = f"DATAX{env}_DATAHUB_FW_SNSR_DB.TBL_SNSR_RSLT"

        ###################################### Batch Section #########################################
        # Error Handling
        assert_results.append(
            ErrorHandling.assert_error_downstream_task_run(
                downstream_tasks=[
                    "TransformFixedDateDeequProfilerPipeline",
                    "TransformStreamingDeequAnomalyPipeline",
                    "TransformStreamingPipeline",
                ],
                workflow_run_id=self.workflow_run_id,
                log_params=["workflow_run_id", "downstream_tasks"],
            )
        )

        assert_results.append(
            ErrorHandling.check_table_not_exist(data_endpoint=batch_output_table)
        )

        assert_results.append(
            ErrorHandling.check_log_failed_detail(
                workflow_run_id=self.workflow_run_id,
                log_endpoint=log_endpoint,
                check_key_word="DeequAuditError",
                log_params=["workflow_run_id", "log_endpoint", "check_key_word"],
            )
        )

        # Test results might be a list of dict, to be created into the result dataframe
        super().update_test_results(
            assert_results, self.configs.test_param, skip_workflow_info_flag=False
        )

    def save_results(self, *args, **kwargs) -> None:
        """
        Save the test results to the specified class object.
        """
        self.configs.result_class.save_results(self.test_results)

    def teardown_resource(self):
        """
        Clean up resources created during setup, including databases, tables, and output files.
        """
        # Teardown each setup table
        for table_class in self.configs.table_classes:
            table_class_obj = table_class(configs=self.configs)
            table_class_obj.teardown()

        # Teardown output table
        for table in self.configs.output_table:
            teardown_table(self.configs.output_database, table)

        # Teardown file system's output
        for path in self.configs.teardown_paths:
            dbutils.fs.rm(path, recurse=True)

    def execute(self) -> None:
        """
        Orchestrates the entire test pipeline by sequentially calling setup_resource(),
        trigger_workflow(), run_test(), save_results(), and teardown_resource() methods.
        """
        self.teardown_resource()
        self.setup_resource()
        self.trigger_workflow(validate_task_success=False)
        self.run_test()
        self.save_results()
        self.teardown_resource()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Tests

# COMMAND ----------

test_task_dq_audit_failure_configs = {
    # required parameters
    "workflow_job_id": 685909697359564,
    "pipeline_class": E2ETestPipelineDQAuditFailure,
    "table_classes": [TableE2ETestDataSourceDQAuditFailure],
    "result_class": E2ETestResults,
    "dl_data_dt": date(2023, 8, 31),
    # optional parameters
    "workflow_input_param": {
        "input_start_date": "2023-08-31",
        "input_end_date": "2023-08-31",
    },
    "input_data_endpoint": f"DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_SOURCE",
    "data_sources_path": data_sources_path,
    "output_database": f"DATAX{env.upper()}_DATAHUB_FWTEST_DB",
    "output_table": ["E2E_TEST_DATA_AGG", "E2E_TEST_DATA_OUTPUT"],
    "teardown_paths": teardown_paths,
    "test_param": {"data_size": [100]},
}

test_task_dq_audit_failure = TestTask(test_task_dq_audit_failure_configs)
test_task_dq_audit_failure.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # E2E Test Pipeline DQ Anomaly Failure

# COMMAND ----------

# MAGIC %md
# MAGIC This test case mocks a data with with a value that will cause an anomaly that fails the rule for the DQ Anomaly after the task 'TransformFixedDatePipeline'
# MAGIC
# MAGIC **Expectation**: The task 'TransformStreamingPipeline' should fail with error indicating that the sensor rule detected that the data is not ready according to the sensor rule. <br>
# MAGIC The record in sensor table should also contain the correct 'is_ready' flag for the failure rule.<br>
# MAGIC **Related test ares**:
# MAGIC - Error Handling
# MAGIC   - check_is_ready_flag_sensor_log
# MAGIC   - check_log_failed_detail
# MAGIC
# MAGIC **Setup Resources**:
# MAGIC - Table: "DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_SOURCE" <br>
# MAGIC Mocking data with end_date according to 'dl_data_dt' and start_date being 3 days prior (for a total of 4 days), 
# MAGIC and mock end of last month data if it is not between start_date and end_date, then union them for the total of 4 or 5 days of data.
# MAGIC Each day of data contains the same row count. <br>
# MAGIC In this case, the column 'uc_id' will contain duplicates, which will cause an anomaly rule that check for uniqueness to flag 'is_ready' as false.
# MAGIC
# MAGIC
# MAGIC - E2E_DATA_AGG's profiler result <br>
# MAGIC Path: "{deequ_analyzer_engine_dir}/DATAX{env.upper()}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/result.json" <br>
# MAGIC Served as the historical data for E2E_DATA_AGG (output from the task 'TransformFixedDatePipeline'), to be used to check in anomaly task 'TransformStreamingDeequAnomalyPipeline'.

# COMMAND ----------

class E2ETestPipelineDQAnomalyFailure(AbstractTestPipeline):
    """
    Represents an end-to-end test pipeline for validating test area. This pipeline sets up resources, runs tests, saves results,
    and then tears down resources after execution.
    """

    def __init__(self, configs) -> None:
        """
        Args:
            configs (TestPipelineConfigs): Configuration object containing pipeline settings.
        """
        super().__init__(configs)

    def setup_resource(self) -> None:
        """
        Set up necessary resources, including databases and tables, for test execution.
        """
        # Setup database (from utils)
        setup_database(self.configs.input_data_endpoint.split(".")[0])

        # Setup table for each table class in the config
        for table_class in self.configs.table_classes:
            table_class_obj = table_class(configs=self.configs)
            table_class_obj.setup()

        # Setup table for historical DQ Profiler result, to be used with DQ Anomaly
        setup_e2e_test_data_agg_profiler_result(dq_profiler_path_data_agg)

    def run_test(self, *args, **kwargs) -> list:
        """
        Execute a series of test cases. Returns a list of test assertion results.
        The assertion results are updated in the class attribute "test_results"
        """
        assert_results = []
        env = os.environ.get("ENVIRONMENT").upper()
        batch_output_table = f"DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_AGG"

        log_endpoint = f"DATAX{env}_DATAHUB_FW_LOG_DB.TBL_ETL_DATA_STS_LOG"
        sensor_log_endpoint = f"DATAX{env}_DATAHUB_FW_SNSR_DB.TBL_SNSR_RSLT"

        ###################################### Batch Section #########################################
        # Error Handling
        assert_results.append(
            ErrorHandling.check_is_ready_flag_sensor_log(
                workflow_run_id=self.workflow_run_id,
                upstream_pipeline="TransformFixedDatePipeline",
                log_endpoint=sensor_log_endpoint,
                expected_snsr_rules= ["AnomalyConstraint(Mean(value,Some(name='Minimum' and instance='class_source_product_sum_transactions_val_dly_v1')))", "AnomalyConstraint(Mean(value,Some(name='Maximum' and instance='class_source_product_sum_transactions_val_dly_v1')))"],
                is_ready_flag=False,
                log_params=["workflow_run_id", "upstream_pipeline", "log_endpoint", "expected_snsr_rules", "is_ready_flag"],
            )
        )

        assert_results.append(
            ErrorHandling.check_log_failed_detail(
                workflow_run_id=self.workflow_run_id,
                log_endpoint=log_endpoint,
                check_key_word="Sensor module detected not-ready prerequisite tasks",
                log_params=["workflow_run_id", "log_endpoint", "check_key_word"],
            )
        )

        # Test results might be a list of dict, to be created into the result dataframe
        super().update_test_results(
            assert_results, self.configs.test_param, skip_workflow_info_flag=False
        )

    def save_results(self, *args, **kwargs) -> None:
        """
        Save the test results to the specified class object.
        """
        self.configs.result_class.save_results(self.test_results)

    def teardown_resource(self):
        """
        Clean up resources created during setup, including databases, tables, and output files.
        """
        # Teardown each setup table
        for table_class in self.configs.table_classes:
            table_class_obj = table_class(configs=self.configs)
            table_class_obj.teardown()

        # Teardown output table
        for table in self.configs.output_table:
            teardown_table(self.configs.output_database, table)

        # Teardown file system's output
        for path in self.configs.teardown_paths:
            dbutils.fs.rm(path, recurse=True)

    def execute(self) -> None:
        """
        Orchestrates the entire test pipeline by sequentially calling setup_resource(),
        trigger_workflow(), run_test(), save_results(), and teardown_resource() methods.
        """
        self.teardown_resource()
        self.setup_resource()
        self.trigger_workflow(validate_task_success=False)
        self.run_test()
        self.save_results()
        self.teardown_resource()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Tests

# COMMAND ----------

test_task_dq_anomaly_failure_configs = {
    # required parameters
    "workflow_job_id": 685909697359564,
    "pipeline_class": E2ETestPipelineDQAnomalyFailure,
    "table_classes": [TableE2ETestDataSourceDQAnomalyFailure],
    "result_class": E2ETestResults,
    "dl_data_dt": date(2023, 8, 31),
    # optional parameters
    "workflow_input_param": {
        "input_start_date": "2023-08-31",
        "input_end_date": "2023-08-31",
    },
    "input_data_endpoint": f"DATAX{env.upper()}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_SOURCE",
    "data_sources_path": data_sources_path,
    "output_database": f"DATAX{env.upper()}_DATAHUB_FWTEST_DB",
    "output_table": ["E2E_TEST_DATA_AGG", "E2E_TEST_DATA_OUTPUT"],
    "teardown_paths": teardown_paths,
    "test_param": {"data_size": [100]},
}

test_task_dq_anomaly_failure = TestTask(test_task_dq_anomaly_failure_configs)
test_task_dq_anomaly_failure.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # Check Logger

# COMMAND ----------

test_logger_path = "/datax/datahub/etl_framework/temp/datax-gp-sample-workflow/test_logger/*/driver/stderr"
test_logger_df = spark.createDataFrame(
    sc.textFile(test_logger_path).map(
        lambda x: tuple([x[0:24], x[24:]])
    ),
    ["time_value", "message"],
)

search_msg = [
    "Launching job", 
    "e2e_test_data_source_anomaly_d3",
    "datax.engine.data_profiler",
    "Complete Deequ anomaly detection",
    "Complete Deequ analyzer",
    "datax.generic_pipeline.sample_workflow",
    "Done Output Validation",
    "Complete Deequ auditing",
    "datax.engine.view_generator",
    "Running view generation"
]

filtered_test_logger_df = test_logger_df.filter(
    col("message").rlike("|".join(search_msg))
)
filtered_test_logger_df.display()
assert filtered_test_logger_df.isEmpty() == False, f"The filtered message from the logger read from the path: {test_logger_path} is empty"
