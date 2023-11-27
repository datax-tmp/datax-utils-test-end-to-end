# Databricks notebook source
# MAGIC %md # Install Libraries

# COMMAND ----------

# MAGIC %pip install importlib-metadata

# COMMAND ----------

# MAGIC %md
# MAGIC #Utilities Functions

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

# MAGIC %md 
# MAGIC #Libraries Import

# COMMAND ----------

# import: standard
import logging
from datetime import date
from datetime import datetime
from datetime import timedelta
from dbldatagen import DateRange
from importlib.metadata import files

# import: datax in-house
from datax.utils.data_quality.decorator.sensor import build_sensor_filter_conditions
from datax.utils.data_quality.deequ.auditor.deequ_auditor import audit_data_quality
from datax.utils.data_quality.sensor.sensor_database import SensorResultTable

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Resources Function

# COMMAND ----------

# MAGIC %md ## Set ENV & Read Config

# COMMAND ----------

import os
from datax.generic_pipeline.sample_workflow.__init__ import __version__

ENVIRONMENT = os.environ.get("ENVIRONMENT").upper()
AZURE_STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT")
os.environ["PACKAGE_VERSION"] = __version__
env = os.environ  # for yml config

configs = ["conf/generic_pipeline/TransformFixedDatePipeline/sensor/sensor_result_table.yml.j2", "conf/generic_pipeline/TransformFixedDatePipeline/audit/deequ/check.yml.j2"]
config_paths = [p.locate() for p in files('datax_gp_sample_workflow') if str(p) in configs]

# COMMAND ----------

from datax.utils.deployment_helper.abstract_class.conf_file_reader import J2Reader

j2_cls = J2Reader(conf_file_paths=config_paths)
j2_conf = j2_cls.read_file()

# COMMAND ----------

# set audit config, activate audit True
AUDIT_CONF = {
    "deequ": {"check": j2_conf["check"]},
    "activate": True,
}

# set sensor config
SENSOR_CONF = j2_conf["sensor_result_table"]

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
                StructField("scb_cust_key", StringType(), True),
                StructField("monoline_key", StringType(), True),
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
        min_scb_cust_key = 10**9
        max_scb_cust_key = (10**10) - 1

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
            .withColumn(
                "scb_cust_key",
                StringType(),
                minValue=min_scb_cust_key,
                maxValue=max_scb_cust_key,
            )
            .withColumn(
                "monoline_key",
                StringType(),
                values=["SCB", "CARDX"],
                random=True,
                weights=[4, 6],
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
                .withColumn(
                    "scb_cust_key",
                    StringType(),
                    minValue=min_scb_cust_key,
                    maxValue=max_scb_cust_key,
                )
                .withColumn(
                    "monoline_key",
                    StringType(),
                    values=["SCB", "CARDX"],
                    random=True,
                    weights=[4, 6],
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
                "tags": {"date": "2023-07-30", "table_name": "E2E_TEST_DATA_AGG"},
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
    dbutils.fs.mkdirs(tmp_profiler_path_data_agg)
    with open(f"/dbfs/{tmp_profiler_path_data_agg}result.json", "w+") as json_file:
        json.dump(mock_analyzer_result, json_file, indent=4)
    dbutils.fs.mv(f"{tmp_profiler_path_data_agg}result.json", dq_profiler_path_data_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC #Function test

# COMMAND ----------

def sensor_process(sensor_conf: dict, start_date: date, end_date: date) -> None:
    target_dates = [start_date, end_date]
    filter_conditions = build_sensor_filter_conditions(sensor_conf, target_dates)
    sensor_table = SensorResultTable()
    sensor_result_df = sensor_table.load_result(
        filter_conditions=filter_conditions, select_latest=True
    )
    sensor_table.check_sensor_is_ready(sensor_result_df)

# COMMAND ----------

def test_query_technical_view() -> None:
    df = spark.table(f"DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB.TECH_VIEW_E2E_TEST_DATA_AGG")
    print(f"Technical view count: {df.count()}")
    

# COMMAND ----------

def test_query_monthly_technical_view() -> None:
    df = spark.table(f"DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB.MONTHLY_TECH_VIEW_E2E_TEST_DATA_AGG")
    print(f"Monthly technical view count: {df.count()}")
    

# COMMAND ----------

def test_query_business_view() -> None:
    df = spark.table(f"DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB.BSNS_VIEW_E2E_TEST_DATA_AGG")
    print(f"Business view count: {df.count()}")


# COMMAND ----------

def test_query_monthly_business_view() -> None:
    df = spark.table(f"DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB.MONTHLY_BSNS_VIEW_E2E_TEST_DATA_AGG")
    df.display()
    print(f"Monthly business view count: {df.count()}")
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Performance Test Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC This test case mocks a transformation workflow that should all the tasks are success. <br>
# MAGIC This workflow will be looped for each 'data_size' in 'test_params' config, using mocking data according to the 'data_size' <br>
# MAGIC After each workflow trigger, 'audit_data_quality' and 'sensor_process' are called and measured time in the function level instead of task level <br>
# MAGIC The results will be recorded on 'task' or 'subprocess' level for each row.
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

class PerformanceTestPipeline(AbstractTestPipeline):
    """
    Represents a performance test pipeline designed to measure and assess the performance
    of specific tasks and functions within a workflow. This pipeline sets up resources,
    measures task and function execution times, saves results, and then tears down resources
    after execution.

    """

    def __init__(self, configs) -> None:
        """
        Args:
            configs (TestPipelineConfigs): Configuration object containing pipeline settings.
        """
        super().__init__(configs)
        self.test_results = []

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

    def run_test(self) -> list:
        """
        Execute performance tests by measuring task and function execution times.
        Returns a list of performance test results.
        """
        # Fix SharedStateHandler has been modified
        from datax.utils.data_layer.handler.shared_state_handler import (
            SharedStateHandler,
        )

        SharedStateHandler._shared_state = dict()
        SharedStateHandler._modified_attrs = set()

        test_results = []

        # Measure and collect task-level execution times
        test_results.extend(
            ElapseTime.get_tasks_results(workflow_run_id=self.workflow_run_id)
        )

        # Measure and collect function-level execution times
        database_name = f"DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB"
        table_name = "E2E_TEST_DATA_AGG"
        df_final_output = spark.read.table(f"{database_name}.{table_name}")
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        ElapseTime.measure_function(audit_data_quality)(
            df=df_final_output,
            conf_audit=AUDIT_CONF,
            table_name=table_name,
            start_date=self.configs.dl_data_dt,
            end_date=self.configs.dl_data_dt,
            logger=logger,
        )
        ElapseTime.measure_function(sensor_process)(
            SENSOR_CONF, self.configs.dl_data_dt, self.configs.dl_data_dt
        )
        ElapseTime.measure_function(test_query_technical_view)()
        ElapseTime.measure_function(test_query_monthly_technical_view)()
        ElapseTime.measure_function(test_query_business_view)()
        ElapseTime.measure_function(test_query_monthly_business_view)()
        test_results.extend(ElapseTime.get_function_results())

        # Update performance test results
        super().update_test_results(test_results, self.configs.test_param)

    def save_results(self, *args, **kwargs) -> None:
        """
        Save the performance test results to the specified class object.
        """
        self.configs.result_class.save_results(self.test_results)

    def teardown_resource(self):
        """
        Clean up resources created during setup, including databases, tables, and files.
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
        Orchestrates the entire performance test pipeline by sequentially calling
        setup_resource(), run_test(), save_results(), and teardown_resource() methods.
        """
        self.teardown_resource()
        self.setup_resource()
        self.trigger_workflow(validate_task_success=False)
        self.run_test()
        self.save_results()
        self.teardown_resource()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path Definitions

# COMMAND ----------

# set up path parameters

if ENVIRONMENT == "SIT" or ENVIRONMENT == "NPR":
    deequ_check_dir = "/deequ-check"
    deequ_analyzer_engine_dir = "/deequ_analyzer"
    fw_sensor_dir = "/fw-sensor"
    input_dir = ""
    output_dir = ""
else:
    deequ_check_dir = f"abfss://deequ-check@{AZURE_STORAGE_ACCOUNT}"
    deequ_analyzer_engine_dir = f"abfss://deequ-analyzer@{AZURE_STORAGE_ACCOUNT}"
    fw_sensor_dir = f"abfss://fw-sensor@{AZURE_STORAGE_ACCOUNT}"
    input_dir = f"abfss://core@{AZURE_STORAGE_ACCOUNT}"
    output_dir = f"abfss://core@{AZURE_STORAGE_ACCOUNT}"

# setup's inputs
data_sources_path = f"{input_dir}/DATAX{ENVIRONMENT.upper()}_DATAHUB_FWTEST_DB"
e2e_test_data_source_path = (
    f"{input_dir}/DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_SOURCE/"
)

# workflow's outputs
dq_audit_path_data_agg = f"{deequ_check_dir}/DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/result.json"
dq_audit_path_data_output = f"{deequ_check_dir}/DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_OUTPUT/result.json"
dq_profiler_path_data_agg = f"{deequ_analyzer_engine_dir}/DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/result.json"
dq_anomaly_result_data_agg = (
    f"/deequ-anomaly/DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/"
)
dq_anomaly_result_data_output = (
    f"/deequ-anomaly/DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_OUTPUT/"
)
e2e_test_data_agg_path = (
    f"{output_dir}/DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/"
)
e2e_test_data_output_path = (
    f"{output_dir}/DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_OUTPUT/"
)

# engine checkpoints
profiler_engine_checkpoint_data_agg = f"{deequ_analyzer_engine_dir}/checkpoint/DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB/E2E_TEST_DATA_AGG/"
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
    # engine checkpoints
    profiler_engine_checkpoint_data_agg,
    sensor_fw_checkpoint_data_source_d3,
    sensor_fw_checkpoint_data_source_m1,
    sensor_fw_checkpoint_data_agg_d1,
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Tests

# COMMAND ----------

test_task_info = {
    # required parameters
    "workflow_job_id": 685909697359564,
    "pipeline_class": PerformanceTestPipeline,
    "result_class": PerformanceTestResults,
    "table_classes": [TableE2ETestDataSource],
    "dl_data_dt": date(2023, 7, 31),
    # optional parameters
    "workflow_input_param": {
        "input_start_date": "2023-07-31",
        "input_end_date": "2023-07-31",
    },
    "input_data_endpoint": f"DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB.E2E_TEST_DATA_SOURCE",
    "data_sources_path": data_sources_path, 
    "output_database": f"DATAX{ENVIRONMENT}_DATAHUB_FWTEST_DB",
    "output_table": ["E2E_TEST_DATA_OUTPUT", "E2E_TEST_DATA_AGG"],
    "teardown_paths": teardown_paths, 
    "test_param": {
        "data_size": [10**8, 3 * (10**8), 9 * (10**8)]
    },
}
test_task = TestTask(test_task_info)
test_task.execute()
