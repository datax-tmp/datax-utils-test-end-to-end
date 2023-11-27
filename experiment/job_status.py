# Databricks notebook source
# MAGIC %md
# MAGIC # Job Log Correctness

# COMMAND ----------

# import: standard
from datetime import date
import os
from typing import Dict
from typing import Optional

# import: pyspark
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.types import Row

# import: datax in-house
from datax.engine.job_launcher.api.api_client import ApiClient

# COMMAND ----------

class JobLogCorrectness(metaclass=TestResultDecorator):
    """
    A class containing methods for validating job status and log information.
    """

    def check_log_period_loading(
        task_name: str,
        target_table: str,
        workflow_run_id: int,
        log_endpoint: str,
        date_column="dl_data_dt",
        **kwargs,
    ) -> str:
        """
        Validates whether the target table's data dates match the log dates for a given workflow run.

        Args:
            task_name (str): The name of the specific task.
            target_table (str): The name of the target table.
            workflow_run_id (int): The ID of the workflow run.
            log_endpoint (str) : log table used for checking
            date_column (str)(optional) : The name of column date to be checked, default value as 'dl_data_dt'

        Returns:
            str: "Success" if validation passes, otherwise raises an assertion error.
        """
        target_dates = [
            str(date[date_column])
            for date in spark.table(target_table)
            .select(date_column)
            .distinct()
            .collect()
        ]

        log_dates = (
            spark.table(log_endpoint)
            .where(f"parent_run_id = '{workflow_run_id}'")
            .where(f"task_nm = '{task_name}'")
            .select("dt_list")
            .first()["dt_list"]
        )

        assert sorted(target_dates) == sorted(
            log_dates
        ), f"Mismatch value, Expected: {target_dates}, Actual: {log_dates}"
        return "Success"

    def check_log_run_detail(
        task_name: str,
        workflow_run_id: int,
        log_endpoint: str,
        expected_count: int,
        **kwargs,
    ) -> str:
        """
        Validates whether expected log information is present for a specific task in a workflow run.

        Args:
            task_name (str): The name of the specific task.
            workflow_run_id (int): The ID of the workflow run.
            log_endpoint (str) : log table used for checking
            expected_count (int) : The number of expected records from log

        Returns:
            str: "Success" if validation passes, otherwise raises an assertion error.
        """
        workflow_info = get_workflow_info(workflow_run_id)
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        workspace_id = (
            workspace_url
            .split(".")[0]
            .replace("adb-", "")
        )
        target_parent_id = workflow_info.job_id
        target_parent_run_id = workflow_info.run_id
        target_job_uri = [
            f"{workspace_url}/?o={workspace_id}#job/{target_parent_id}/run/{item.run_id}"
            for item in workflow_info.tasks
            if item.task_key == task_name
        ][0]
        log_outputs = (
            spark.table(log_endpoint)
            .where(
                f"""parent_id = '{target_parent_id}' 
                              and parent_run_id = '{target_parent_run_id}'
                              and job_uri = '{target_job_uri}'
                              and task_nm = '{task_name}' """
            )
            .count()
        )
        assert (
            log_outputs == expected_count
        ), f"The number of logs found for task name: {task_name} does not match the expected number of logs: {expected_count}"
        return "Success"
    
    def check_log_information_match_expected(
        job_log_table_name: str,
        product: str,
        job_start_dt: date,
        parent_run_id: int,
        task_name: str,
        expected_log_details: Dict,
        job_log_database_name: Optional[str] = f"DATAX{os.environ['ENVIRONMENT'].upper()}_DATAHUB_FW_LOG_DB",
        **kwargs,
    ) -> str:
        """
        Check the correctness of job log details against the expected values.

        Args:
            job_log_table_name (str): The name of the job log table to be queried.
            product (str): The product name for filtering.
            job_start_dt (date): The job start date for filtering.
            parent_run_id (int): The parent run ID for filtering.
            task_name (str): The name of the task for filtering.
            expected_log_details (Dict): A dictionary of expected log details.
            job_log_database_name (Optional[str]): The name of the job log database to be queried. Default to f"DATAX{os.environ['ENVIRONMENT'].upper()}_DATAHUB_FW_LOG_DB".
            **kwargs: Additional keyword arguments. 

        Returns:
            str: A string indicating the result of the check (e.g., "Success" or an error message).
        """

        # Check if the specified table exists
        if not spark.catalog.tableExists(f"{job_log_database_name}.{job_log_table_name}"):
            raise ValueError(f"Table '{job_log_database_name}.{job_log_table_name}' not found")

        # Read the job log table
        job_log_table = spark.table(f"{job_log_database_name}.{job_log_table_name}")

        # Filter the job log table to select the specific records
        filter_job_log_table = (
            job_log_table.where(col("prdct") == product)
            .where(col("job_strt_dt") == job_start_dt)
            .where(col("parent_run_id") == parent_run_id)
            .where(col("task_nm") == task_name)
        )

        # Check if the filtered table is empty
        if filter_job_log_table.isEmpty():
            raise ValueError("With the given querying, job log table returns no result")

        # Extract the list of keys from the expected log details
        list_expected_keys = list(expected_log_details.keys())

        # Select the first record from the filtered table
        job_log_record = filter_job_log_table.select(list_expected_keys).first()

         # Define an empty dictionary to store actual log information
        actual_log_info = {}
        for idx, header in enumerate(list_expected_keys):
            value = job_log_record[idx]
            # If the value is a Row object, convert it to a dictionary
            if isinstance(value, Row):
                value = value.asDict()
            actual_log_info[header] = value

        # Compare the actual log information with the expected log details
        assert (
            actual_log_info == expected_log_details
        ), f"Log details is mismatch. The actual job log info is: {actual_log_info}"
        return "Success"

    def check_log_cstm_val(
            task_name: str,
            workflow_run_id: int,
            log_endpoint: str,
            check_key_word: str,
            **kwargs,
    ) -> str:
        """
        Validates whether expected key word is contained in the 'cstm_val' column in log table

        Args:
            task_name (str): The name of the specific task.
            workflow_run_id (int): The ID of the workflow run.
            log_endpoint (str) : log table used for checking
            check_key_word (str) : They keyword used to search in cstm_val column

        Returns:
            str: "Success" if validation passes, otherwise raises an assertion error.
        """
        workflow_info = get_workflow_info(workflow_run_id)
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        workspace_id = (
            workspace_url
            .split(".")[0]
            .replace("adb-", "")
        )
        target_parent_id = workflow_info.job_id
        target_parent_run_id = workflow_info.run_id
        target_job_uri = [
            f"{workspace_url}/?o={workspace_id}#job/{target_parent_id}/run/{item.run_id}"
            for item in workflow_info.tasks
            if item.task_key == task_name
        ][0]
        # explode the array of struct 'cstm_val' and search each struct for the keyword
        boolean_check_result = (
            spark.table(log_endpoint)
            .where(
                f"""parent_id = '{target_parent_id}' 
                              and parent_run_id = '{target_parent_run_id}'
                              and job_uri = '{target_job_uri}'
                              and task_nm = '{task_name}' """
            )
            .select("cstm_val", explode(col("cstm_val")).alias("cstm_val_struct"))
            .filter(f"cstm_val_struct.val like '%{check_key_word}%'")
            .isEmpty()
        )
        assert (
            not boolean_check_result
        ), f"There is no records in log table containing key word: {check_key_word}"
        return "Success"
