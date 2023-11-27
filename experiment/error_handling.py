# Databricks notebook source
from pyspark.sql.functions import collect_list

# COMMAND ----------

class ErrorHandling(metaclass=TestResultDecorator):
    """
    A class containing methods for validating error jobs' handlings

    """

    def assert_error_downstream_task_run(
        downstream_tasks: list, workflow_run_id: int, **kwargs
    ) -> str:

        """
        Check if the downstream tasks of the failed task are skipped

        Parameters:
            downstream_tasks (list): The tasks that depend on the failing task
            workflow_run_id (int): The run_id of a workflow run

        Returns:
            str: A success message if the check passes.

        Raises:
            1. AssertionError: If the downstream tasks' 'life_cycle_state' is not 'SKIPPED'
            2. AssertionError: If the downstream tasks' 'result_state' is not 'UPSTREAM_FAILED'
        """

        workflow_info = get_workflow_info(workflow_run_id)
        for task in workflow_info.tasks:
            task_key = task.task_key
            if task_key in downstream_tasks:
                task_state = task.state
                assert (
                    task_state.life_cycle_state.value == "SKIPPED"
                ), f"The task: {task_key}'s life_cycle_state does not match the expected value: 'SKIPPED'"
                assert (
                    task_state.result_state.value == "UPSTREAM_FAILED"
                ), f"The task: {task_key}'s result_state does not match the expected value: UPSTREAM_FAILED"

        return "Success"

    def check_table_not_exist(data_endpoint: str, **kwargs):
        """
        Checks if the table specified in 'data_endpoint' exists

        Parameters:
            data_endpoint (str): The data endpoint of the table to check.

        Returns:
            str: A success message if the check passes.

        Raises:
            AssertionError: If the specified table exist, an AssertionError is raised.

        Example:
            check_table_not_exist("my_db.my_table")
        """
        boolean_check_result = spark.catalog._jcatalog.tableExists(data_endpoint)
        assert not boolean_check_result, f"Table exist {data_endpoint}."

        return "Success"

    def check_log_failed_detail(
        workflow_run_id: int, log_endpoint: str, check_key_word: str, **kwargs
    ):
        """
        Checks if there are any records in the log table that contain a specified keyword
        and are associated with a given workflow run ID.

        Parameters:
            workflow_run_id (int): The ID of the workflow run.
            log_endpoint (str): log table used for checking
            check_key_word (str): The keyword to search for in the log messages.

        Returns:
            str: A success message if the check passes.

        Raises:
            AssertionError: If no matching records are found in the log table, an AssertionError is raised.

        Example:
            check_log_failed_detail(12345, "error")

        """
        boolean_check_result = (
            spark.table(log_endpoint)
            .filter(f"parent_run_id = '{workflow_run_id}'")
            .filter(f"err_msg like '%{check_key_word}%' ")
            .isEmpty()
        )
        assert (
            not boolean_check_result
        ), f"There is no records in log table containing key word: {check_key_word} "
        return "Success"

    def check_is_ready_flag_sensor_log(
        workflow_run_id: int,
        upstream_pipeline: str,
        log_endpoint: str,
        expected_snsr_rules: list,
        is_ready_flag=False,
        **kwargs,
    ) -> str:

        """
        Checks if there are any records in the sensor log table that have the specified 'is_ready' flag
        and are associated with a given workflow run ID.

        Parameters:
            workflow_run_id (int): The ID of the workflow run.
            log_endpoint (str): log table used for checking
            is_ready_flag (bool, optional): The is_ready flag to filter by (default is False).
            expected_snsr_rules (list) : list of sensor rule needed to be check

        Returns:
            str: A success message if the check passes.

        Raises:
            AssertionError: If no matching records are found in the sensor log table, an AssertionError is raised.

        Example:
            check_is_ready_flag_sensor_log(12345, True)
        """

        actual_snsr_rules = [
            row["snsr_rule"]
            for row in spark.table(log_endpoint)
            .filter(f"parent_run_id = '{workflow_run_id}'")
            .filter(f"upstrm_pl = '{upstream_pipeline}'")
            .filter(f"is_ready is {is_ready_flag}")
            .select("snsr_rule")
            .collect()
        ]
        assert set(actual_snsr_rules) == set(
            expected_snsr_rules
        ), f"Found mismatch results , Actual results : {actual_snsr_rules}, Expected results : {expected_snsr_rules}"
        return "Success"
