# Databricks notebook source
from pyspark.sql.functions import col
import ast

# COMMAND ----------

class SensorResult(metaclass=TestResultDecorator):
    """
    A class containing methods for validating data anomaly's sensor table

    """

    def check_deequ_sensor_result_table(
        workflow_run_id: int,
        upstream_pipeline: str,
        expected_values: list,
        column_name: str = "dt",
        **kwargs,
    ) -> str:

        """
        Check if the sensor result table contains the expected values from the 'upstream_pipeline' record.

        Parameters:
            workflow_run_id (int): The path to the Deequ results file.
            upstream_pipeline (list): Name of the updstream pipeline.
            expected_values (list): List of expected string values
            column_name (str, optional): The name of the string column. Default is 'date'.

        Returns:
            str: A success message if the check passes.

        Raises:
            AssertionError: If the values in the string column don't match the expected values.
        """
        # query sensor result table as a dataframe
        sensor_result_df = spark.table(
            f"DATAX{env.upper()}_DATAHUB_FW_SNSR_DB.TBL_SNSR_RSLT"
        )
        sensor_result_dates = (
            sensor_result_df.filter(
                (col("parent_run_id") == workflow_run_id)
                & (col("upstrm_pl") == upstream_pipeline)
            )
            .select(column_name)
            .distinct()
            .collect()
        )
        # extract date from the column specified in the parameter 'column_name' 
        # convert result to string because in the sensor result table,
        # the date recorded is in date type
        target_values = [str(row[column_name]) for row in sensor_result_dates]

        # compare date
        assert set(target_values) == set(
            expected_values
        ), f"Mismatch values, target_values : {target_values}, expected_values : {expected_values}"
        return "Success"
