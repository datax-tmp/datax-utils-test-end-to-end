# Databricks notebook source
# MAGIC %md
# MAGIC # Data Correctness

# COMMAND ----------

# import: standard
from datetime import date
from datetime import timedelta
from decimal import Decimal
from typing import Dict
from typing import List
from typing import Union

# import: pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import max
from pyspark.sql.functions import min


class DataCorrectness(metaclass=TestResultDecorator):
    """
    A class containing methods for validating output dataframe correctness.

    """

    def check_column_no_null_values(
        df: DataFrame, column_name: str, **kwargs
    ) -> str:
        """
        Check if a non-nullable column has null value(s).

        This function takes a DataFrame and a column name, and checks if the specified column
        contains any null values. It raises an assertion error if null values are found,
        indicating the presence of null values in a non-nullable column.

        Args:
            df (DataFrame): The DataFrame to be checked.
            column_name (str): The name of the column to be checked for null values.

        Returns:
            str: A success message if no null values are found.

        Raises:
            AssertionError: If the specified column contains null values.

        Example:
            >>> check_column_no_null_values(df, "age")
            'Success'
        """
        # Raise an assertion error if null values are found
        assert df.where(
            col(column_name).isNull()
        ).isEmpty(), f"Column '{column_name}' contains null value(s)"
        return "Success"

    def check_column_no_duplicate_values(
        df: DataFrame, column_name: str, **kwargs
    ) -> str:
        """
        Check if a given column(s) is unique.

        This function takes a DataFrame and a column name, and checks if the values in the
        specified column are unique. It raises an assertion error if duplicate values are found
        in the column, indicating that the column is not unique.

        Args:
            df (DataFrame): The DataFrame to be checked.
            column_name (str): The name of the column to be checked for uniqueness.

        Returns:
            str: A success message if the column has unique values.

        Raises:
            AssertionError: If duplicate values are found in the specified column.

        Example:
            >>> check_column_no_duplicate_values(df, "employee_id")
            'Success'
        """
        distinct_df = df.select(column_name).distinct()

        assert (
            df.count() == distinct_df.count()
        ), f"Column '{column_name}' contains duplicate value(s)"
        return "Success"

    def check_column_all_values_are_equal(
        df: DataFrame, column_name: str, **kwargs
    ) -> str:
        """
        Check if a given column(s) is distinct.

        This function takes a DataFrame and a column name, and checks if the values in the
        specified column is distinct (have only 1 value). It raises an assertion error if more than 1 values are found
        in the column, indicating that the column is not distinct.

        Args:
            df (DataFrame): The DataFrame to be checked.
            column_name (str): The name of the column to be checked for distinctness.

        Returns:
            str: A success message if the column is distinct.

        Raises:
            AssertionError: If multiple values are found in the specified column.

        Example:
            >>> check_column_all_values_are_equal(df, "date")
            'Success'
        """
        assert (
            df.select(column_name).distinct().count() == 1
        ), f"Column '{column_name}' contains multiple values"

        return "Success"

    def check_table_schema(
        database_name: str, table_name: str, expected_schema: Dict, **kwargs
    ) -> str:
        """
        Check if a target table's schema matches with the expected column types.

        This function takes a DataFrame and an expected schema, and checks if the actual
        schema of the DataFrame matches the expected schema in terms of column names and data types.
        It raises an assertion error if any inconsistencies are found.

        Args:
            database_name (str): Target database name
            table_name (str): Target table name
            expected_schema (Dict): A dictionary specifying the expected column names and their data types.

        Returns:
            str: A success message if the DataFrame's schema matches the expected schema.

        Raises:
            AssertionError: If the DataFrame's schema does not match the expected schema.

        Example:
            >>> database_name = "dataxmdl_datahub_mlsys_feat_db"
            >>> table_name = "feat_data_fe_datax_incm_prxy_cust_basic_mnth"
            >>> expected_schema = {
                    "job_nm": StringType(),
                    "area_nm": StringType(),
                    "task_nm": StringType(),
                    "prdct": StringType(),
                    "task_type": StructType(
                        [
                            StructField("task", StringType(), True),
                            StructField("subtask", StringType(), True),
                        ]
                    ),
                    "relse_vrsn": StructType(
                        [
                            StructField("relse_vrsn", StringType(), True),
                            StructField("major", StringType(), True),
                            StructField("minor", StringType(), True),
                            StructField("rvsn", StringType(), True),
                            StructField("pre_relse", StringType(), True),
                        ]
                    ),
                    "parent_id": StringType(),
                    "parent_run_id": StringType(),
                    "run_id": StringType(),
                    "parent_job_uri": StringType(),
                    "job_uri": StringType(),
                    "job_strt_dt": DateType(),
                    "job_strt_dttm": TimestampType(),
                    "job_end_dt": DateType(),
                    "job_end_dttm": TimestampType(),
                    "job_drtn": FloatType(),
                    "exec_usr": StringType(),
                    "job_sts": StringType(),
                    "err_msg": StringType(),
                    "cstm_val": ArrayType(StructType([StructField("val", StringType(), True)]), True),
                    "dl_data_dt": ArrayType(StringType(), True),
                    "load_type": StringType(),
                    "dt_col": StringType(),
                    "dt_list": ArrayType(StringType(), True),
                }
            >>> check_table_schema(database_name, table_name, expected_schema)
            'Success'
        """

        df = spark.table(f"{database_name}.{table_name}")
        actual_schema = {column.name: column.dataType for column in df.schema}
        assert (
            actual_schema == expected_schema
        ), "Actual schema is not matched with expected"
        return "Success"

    def check_column_list_of_values(
        df: DataFrame,
        column_name: str,
        expected_value: List,
        exclude_null: bool = True,
        **kwargs,
    ) -> str:
        """
        Check if the distinct values of a column match the expected values.

        This function takes a DataFrame, a column name, and a list of expected values,
        and checks if the distinct values in the specified column match the expected values.
        It raises an assertion error if any inconsistencies are found.

        Args:
            df (DataFrame): The DataFrame to be checked.
            column_name (str): The name of the column to be checked.
            expected_value (list): A list of expected distinct values for the column.
            exclude_null (bool): An indicator whether to exclude null value when checking with expected.

        Returns:
            str: A success message if the distinct values in the column match the expected values.

        Raises:
            AssertionError: If the distinct values in the column do not match the expected values.

        Example:
            >>> expected_values = [1, 2, 3, 4, 5]
            >>> check_column_list_of_values(df, "rating", expected_values)
            'Success'
        """
        if exclude_null:
            df = df.where(col(column_name).isNotNull())

        set_distinct_value = {
            row[column_name] for row in df.select(column_name).distinct().collect()
        }
        
        assert set_distinct_value.issubset(
            set(expected_value)
        ), f"Actual list of value of column '{column_name}' is not matched with expected list of value"

        return "Success"

    def check_column_range_of_values(
        df: DataFrame,
        column_name: str,
        expected_min_boundary: Union[int, float, Decimal],
        expected_max_boundary: Union[int, float, Decimal],
        **kwargs,
    ) -> str:
        """
        Validates if the range of values in the specified DataFrame column
        falls within the expected minimum and maximum boundaries.

        Args:
            df (DataFrame): The input DataFrame.
            column_name (str): The name of the column to be checked.
            expected_min_boundary (Union[int, float, Decimal]): The expected minimum value boundary for the column.
            expected_max_boundary (Union[int, float, Decimal]): The expected maximum value boundary for the column.

        Returns:
            str: Returns a message indicating the validation result.

        Raises:
            AssertionError: If the actual min-max boundary is outside the expected min-max boundary range.

        Example:
            >>> expected_min_boundary = 10
            >>> expected_max_boundary = 1_000_000_000
            >>> check_column_list_of_values(df, "post_amt", expected_min_boundary, expected_max_boundary)
            'Success'
        """
        # Calculate actual minimum and maximum values on the specified column
        actual_min_boundary, actual_max_boundary = (
            df.where(col(column_name).isNotNull())
            .select(
                min(col(column_name)).alias("actual_min"),
                max(col(column_name)).alias("actual_max"),
            )
            .first()
        )

        # Check if actual boundaries are within expected boundaries
        is_min_accept = actual_min_boundary >= expected_min_boundary
        is_max_accept = actual_max_boundary <= expected_max_boundary

        # Raise an error if the boundaries are not within the expected range
        assert (
            is_min_accept and is_max_accept
        ), "Actual range is outside the expected range for minimum and maximum values."

        return "Success"

    def check_output_table_partitions(
        target_table: str,
        expected_values: list,
        column_partition: str = "dl_data_dt",
        **kwargs,
    ) -> str:
        """
        Validates whether the target table contains the expected data dates.

        Args:
            target_table (str): The name of the target table.
            expected_values (list): List of expected data dates.
            column_partition (str)(optional) : The name of column partition, default value as 'dl_data_dt'

        Returns:
            str: "Success" if validation passes, otherwise raises an assertion error.

        Raises:
            AssertionError: If the target table does not contains the partition value date as the expected values
        """
        target_partition_dates = (
            spark.sql(f"show partitions {target_table}")
            .select(column_partition)
            .filter(col(column_partition).isin(expected_values))
            .collect()
        )
        target_partition_dates = [
            date[column_partition] for date in target_partition_dates
        ]

        assert set(target_partition_dates) == set(
            expected_values
        ), f"Mismatch value, Expected: {expected_values}, Actual: {target_partition_dates}"

        return "Success"

    def check_date_period_loading(
        target_table: str,
        start_date: date,
        end_date: date,
        column_date="dl_data_dt",
        **kwargs,
    ) -> str:
        """
        Validates whether the target stream table contains data for the expected period.

        Args:
            target_table (str): The name of the target stream table.
            start_date (str): Start date of the expected data period (format: "YYYY-MM-DD").
            end_date (str): End date of the expected data period (format: "YYYY-MM-DD").
            column_date (str)(optional) : The name of column date to be checked, default value as 'dl_data_dt'

        Returns:
            str: "Success" if validation passes, otherwise raises an assertion error.
        """
        expected_dates = []
        current_date = start_date
        while current_date <= end_date:
            expected_dates.append(current_date)
            current_date += timedelta(days=1)

        target_dates = (
            spark.table(target_table)
            .select(column_date)
            .filter((col(column_date) >= start_date) & (col(column_date) <= end_date))
            .distinct()
            .collect()
        )
        target_dates = [date[column_date] for date in target_dates]

        assert sorted(target_dates) == sorted(
            expected_dates
        ), f"Mismatch value, Expected: {expected_dates}, Actual: {target_dates}"
        return "Success"

    def check_data_equality(
        expected_df: DataFrame, data_endpoint: str, **kwargs
    ) -> str:
        """
        Check data equality between an expected DataFrame and the data in the testing table

        Parameters:
            expected_df (DataFrame): The expected DataFrame used to compare the table's data
            data_endpoint (str): Endpoint to the data table. Format: <database_name>.<table_name>

        Returns:
            str: A success message if the check passes.

        Raises:
            AssertionError: If the DataFrame and table data does not have equal schema and data
        """

        # query data from the table
        actual_df = spark.table(data_endpoint)

        assert (
            expected_df.schema == actual_df.schema
        ), f"The schema of the expected DataFrame: {expected_df.schema} does not match the actual DataFrame: {actual_df.schema}"

        assert (
            expected_df.collect() == actual_df.collect()
        ), "The data from the expected DataFrame does not match the actual DataFrame"
        
        return "Success"

