# Databricks notebook source
from pyspark.sql.functions import col
import ast
from typing import Set

# COMMAND ----------

class DeequCorrectness(metaclass=TestResultDecorator):
    """
    A class containing methods for validating data quality outputs' correctness

    """

    def check_deequ_array_column(
        path: str, expected_values: list, column_name="dt_list", **kwargs
    ) -> str:

        """
        Check correctness of a Deequ array column against expected values.

        Parameters:
            path (str): The path to the Deequ results file.
            expected_values (list): List of expected array values.
            column_name (str, optional): The name of the array column. Default is 'dt_list'.

        Returns:
            str: A success message if the check passes.

        Raises:
            AssertionError: If the values in the array column don't match the expected values.
        """

        # read deequ file
        deequ_results_df = read_deequ_to_df(path)

        # take the first record since every record contains the same "dataset_date"
        target_values = (
            deequ_results_df
            .select(column_name)
            .first()[column_name]
        )

        # check data type of the 'target_values' if it is a string and convert to list
        if isinstance(target_values, str) == True:
            target_dates = ast.literal_eval(target_values)
            if isinstance(target_dates, list) != True:
                raise f"The target dates is not a list type"
        else:
            raise f"The target values: {target_values} is not a string type"

        # compare target_values' dates and expected_values' dates
        assert set(target_dates) == set(
            expected_values
        ), f"Mismatch values, target_dates: {target_dates}, expected_values : {expected_values}"
        return "Success"

    def check_deequ_string_column(
        path: str, expected_values: list, column_name="date", **kwargs
    ) -> str:

        """
        Check correctness of a Deequ string column against expected values.

        Parameters:
            path (str): The path to the Deequ results file.
            expected_values (list): List of expected string values.
            column_name (str, optional): The name of the string column. Default is 'date'.

        Returns:
            str: A success message if the check passes.

        Raises:
            AssertionError: If the values in the string column don't match the expected values.
        """
        # read deequ
        deequ_results_df = read_deequ_to_df(path)
        # extract date
        target_values = [
            row[column_name]
            for row in deequ_results_df.select(column_name)
            .filter(col(column_name).isin(expected_values))
            .distinct()
            .collect()
        ]

        # compare date
        assert set(target_values) == set(
            expected_values
        ), f"Mismatch values, target_values : {target_values}, expected_values : {expected_values}"
        return "Success"

    def check_deequ_profiling_results_match_expected(
        path: str,
        data_date: str,
        data_column: str,
        expected_config_info: Set,
        **kwargs,
    ) -> str:
        """
        Check correctness of Deequ profiling results against expected values.

        Parameters:
            path (str): The path to the Deequ results file.
            data_date (str): The date for which the data is checked.
            data_column (str): The name of the data column.
            expected_config_info (set): A set of expected configuration values.

        Returns:
            str: A success message if the check passes.

        Raises:
            FileNotFoundError: If the path is incorrect or the Deequ information is empty.
            AssertionError: If the values in the Deequ profiling results don't match the expected values.

        Example:
            >>> path = "abfss://deequ-analyzer@<storage-account>.dfs.core.windows.net/<db_name>/<table_name>/result.json"
            >>> expected_config_info = {
                                        "ApproxQuantiles-0.75",
                                        "ApproxQuantiles-0.25",
                                        "Completeness",
                                        "Mean",
                                        "Sum",
                                        "StandardDeviation",
                                    }
            >>> check_deequ_profiling_results_match_expected(path=path,data_date='2023-07-31',data_column='featureA',expected_config_info=expected_config_info)
            'Success'
        """

        # Read Deequ file as a PySpark DataFrame
        deequ_results_df = read_deequ_to_df(path)

        if deequ_results_df.isEmpty():
            raise FileNotFoundError(
                "Either the information from path is empty or the path given is incorrect."
            )
        # Filter by data date
        deequ_results_df = deequ_results_df.filter(col("date") == data_date)

        # Filter by max running timestamp
        max_date = deequ_results_df.agg({"dataset_date": "max"}).first()[0]
        deequ_results_df = deequ_results_df.filter(col("dataset_date") == max_date)

        # Filter by data column
        deequ_results_df = deequ_results_df.filter(col("instance") == data_column)

        # Extract actual profiling set from DataFrame
        actual_profiling_set = set([row.name for row in deequ_results_df.collect()])

        # Compare actual profiling set with expected values
        assert actual_profiling_set == expected_config_info, (
            f"Result from Deequ mismatch.\n"
            f"Expected: {expected_config_info}\n"
            f"Got: {actual_profiling_set}"
        )

        return "Success"
