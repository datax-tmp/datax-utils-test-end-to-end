# Databricks notebook source
# MAGIC %md
# MAGIC # Libraries Installation

# COMMAND ----------

pip install databricks.sdk

# COMMAND ----------

pip install pydantic

# COMMAND ----------

pip install dbldatagen

# COMMAND ----------

# MAGIC %md 
# MAGIC #Libraries Import

# COMMAND ----------

# import: standard
import itertools
import json
import os
import requests
import time
import timeit
from abc import ABC
from abc import abstractmethod
from datetime import datetime
from datetime import date
from inspect import getmembers
from inspect import isclass
from typing import Callable
from typing import Type
from typing import Union
from pydeequ.repository import FileSystemMetricsRepository

# import: pyspark
from pyspark.sql.types import DateType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

# import: external
import dbldatagen as dg
from pydantic import BaseModel
from pydantic import Extra
from pydantic import validator

from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md
# MAGIC #Utilities Functions

# COMMAND ----------

def read_deequ_to_df(path: str):
    """
    Method to convert deequ json into spark dataframe

    Args:
        path (str): json path

    Returns:
        dataframe : spark dataframe
    """
    repository = FileSystemMetricsRepository(spark, path)
    deequ_results_df = repository.load().getSuccessMetricsAsDataFrame()
    return deequ_results_df

# COMMAND ----------

def unix_to_timestamp(unix_timestamp: int) -> datetime:
    """
    Method to convert unixsttamp format into datetime format

    Args:
        unix_timestamp (int): unix timstamp

    Returns:
        datetime: value as datetime format
    """
    # Convert to seconds if the length is greater than 10 digits
    if len(str(unix_timestamp)) > 10:
        timestamp = unix_timestamp / 10 ** (len(str(unix_timestamp)) - 10)
    else:
        timestamp = unix_timestamp

    # Check if the timestamp length is less than 10, pad with zeros if necessary
    if len(str(unix_timestamp)) < 10:
        timestamp = timestamp * (10 ** (10 - len(str(timestamp))))
    dt = datetime.fromtimestamp(timestamp)
    
    return dt

# COMMAND ----------

def get_workflow_info(run_id: int) -> object:
    """
    Method that calls to get a workflow run's information according to 'workflow_run_id'.

    Args:
        run_id (int): workflow's run_id

    Returns:
        object: object of workflow run information
    """
    w = WorkspaceClient()
    workflow_info = w.jobs.get_run(run_id=run_id)
    
    return workflow_info

# COMMAND ----------

def copy_function_attributes(
    func_wrapper: Callable, func_original: Callable
) -> Callable:
    """
    Use for setting atrribute function name and class name from one function to another function.

    Parameters:
        func_wrapper (Callable): adding attribute function
        func_original (Callable): source of attribute function

    Returns:
        Callable: A function with attribute function name and class name from the another function
    """
    class_name = (
        func_original._class_name
        if hasattr(func_original, "_class_name")
        else func_original.__qualname__.split(".")[0]
    )
    function_name = (
        func_original._function_name
        if hasattr(func_original, "_function_name")
        else func_original.__name__
    )
    varnames = (
        func_original._varnames
        if hasattr(func_original, "_varnames")
        else func_original.__code__.co_varnames[: func_original.__code__.co_argcount]
    )
    setattr(func_wrapper, "_function_name", function_name)
    setattr(func_wrapper, "_class_name", class_name)
    setattr(func_wrapper, "_varnames", varnames)
    
    return func_wrapper

# COMMAND ----------

def get_test_results_dict(func: Callable) -> Callable:
    """
    Used as a decorator function to generate dict test results contaning class name , function name, assertion result. The returned dictionary can be used
    for reporting and logging test outcomes.

    Parameters:
        func (Callable): The function to be wrapped.

    Returns:
        Callable: A wrapped function that returns a dictionary with test results and metadata.
    """
    class_name = (
        func._class_name
        if hasattr(func, "_class_name")
        else func.__qualname__.split(".")[0]
    )
    function_name = (
        func._function_name if hasattr(func, "_function_name") else func.__name__
    )

    varnames = (
        func._varnames
        if hasattr(func, "_varnames")
        else func.__code__.co_varnames[: func.__code__.co_argcount]
    )

    def wrapper(*args, **kwargs):

        # retrieve argument name and value
        if "log_params" in kwargs:
            param_dict = {varnames[index]: arg for index, arg in enumerate(args)}
            param_dict.update(kwargs)
            log_params_dict = str(
                {
                    key: value
                    for key, value in param_dict.items()
                    if key in kwargs["log_params"]
                }
            )
        else:
            log_params_dict = None

        test_results_dict = {}
        test_results_dict["test_area"] = class_name
        test_results_dict["assertions"] = function_name
        test_results_dict["assertion_result"] = func(*args, **kwargs)
        test_results_dict["log_params"] = log_params_dict
        return test_results_dict

    return wrapper

# COMMAND ----------

def try_except_error_handling(func: Callable) -> str:
    """
    Wraps a function with error handling and returns an informative error message.

    This function wraps the provided function with a try-except block to capture
    AssertionError and other exceptions. It returns an error message containing
    information about the caught error, which can be helpful for debugging.

    Parameters:
        func (Callable): The function to be wrapped.

    Returns:
        str: An error message in case of an exception, or the result of the wrapped function.
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AssertionError as e:
            return f"AssertionError: {e}"
        except Exception as e:
            return str(e)

    # Set attributes to the function for use in get_test_results_dict
    return copy_function_attributes(wrapper, func)

# COMMAND ----------

# validate configurations
class TaskInputValidation(BaseModel, extra=Extra.allow):
    """
    Represents the input validation for a task's configuration.

    This class defines the input validation rules and structure for various configuration
    parameters used in a task. It uses Pydantic's BaseModel for easy validation and type checking.

    Attributes:
        workflow_job_id (int): The ID of the workflow job.
        pipeline_class (Type): The class representing the pipeline.
        result_class (Type): The class representing the result.
        dl_data_dt (str): The Business date in format (yyyy-MM-dd).
        table_classes (list[Type], optional): List of table classes.
        workflow_input_param (dict, optional): Additional parameters for the workflow's notebook_param.
        workflow_python_param (list, optional): Additional parameters for the workflow's python_param.
        input_data_endpoint (str, optional): The table name of input data.
        output_database (str, optional): The output database.
        output_table (str, list[str], optional): The output table.
        test_param (dict, optional): Test parameters.

    Methods:
        validate_workflow_id(cls, workflow_job_id): Validates the workflow job ID.

    Usage:
        input_data = {
            "workflow_job_id": 123,
            "pipeline_class": MyPipeline,
            "result_class": MyResult,
            "dl_data_dt": "2023-08-15",
            # ... other parameters ...
        }
        validation_result = TaskInputValidation(**input_data)
    """

    workflow_job_id: int
    pipeline_class: Type
    result_class: Type
    dl_data_dt: Union[str, date]
    table_classes: list[Type] = None
    workflow_input_param: dict = None
    workflow_python_param: list = None
    input_data_endpoint: str = None
    output_database: str = None
    output_table: Union[str, list[str]] = None
    test_param: dict = None

    @validator("workflow_job_id")
    def validate_workflow_id(cls, workflow_job_id):
        # Databricks' auto generated workflow's job ID will always be a positive value
        if workflow_job_id <= 0:
            raise ValueError("workflow_job_id must be a positive integer")
        return workflow_job_id

# COMMAND ----------

# MAGIC %md
# MAGIC #JobWorkspaceClient

# COMMAND ----------

class JobWorkspaceClientError(RuntimeError):
    """
    Custom exception class for errors related to the JobWorkspaceClient.

    This exception is raised when there are errors or issues encountered while interacting with Databricks jobs
    using the JobWorkspaceClient class.

    Inherits from the standard Python `RuntimeError` class.

    Attributes:
        message (str): A human-readable error message describing the specific issue.
    """

    pass


class JobWorkspaceClient:
    """
    Attributes:
        MAX_WAIT_TIME (int): Maximum time to wait for job completion in seconds (default: 7200 seconds).
        WAIT_TIME (int): Time interval between polling for job status in seconds (default: 60 seconds).

    """

    MAX_WAIT_TIME: [int] = 7200
    WAIT_TIME: [int] = 60

    def wait_for_job_completion(self, run_id: str) -> dict:
        """
        Wait for the completion of a Databricks job.

        Args:
            run_id (str): The unique identifier of the job run.

        Returns:
            dict: The response containing job run information.

        Raises:
            JobWorkspaceClientError: If the job does not complete within the specified time or encounters an error.
        """
        w = WorkspaceClient()
        start_time = time.time()

        while True:
            response = w.jobs.get_run(run_id=run_id)
            status = response.state.life_cycle_state.value
            if status not in ["PENDING", "RUNNING"]:
                break

            elapsed_time = time.time() - start_time
            if elapsed_time >= self.MAX_WAIT_TIME:
                break

            time.sleep(self.WAIT_TIME)

        return response

    def validate_task_success(self, run_id: str) -> None:
        """
        Validate the success of tasks in a completed Databricks job.

        Args:
            run_id (str): The unique identifier of the job run.

        Raises:
            JobWorkspaceClientError: If any task in the job run fails.
        """
        w = WorkspaceClient()
        response = w.jobs.get_run(run_id=run_id)
        response_state = response.state
        life_cycle_state = response_state.life_cycle_state.value

        if life_cycle_state != "TERMINATED":
            raise JobWorkspaceClientError(
                f"Result life cycle state of run_name:{response.run_name}, run_id:{response.run_id}, run_page_url:{response.run_page_url} is {life_cycle_state}."
            )

        failed_tasks = []
        for task in response.tasks:
            task_state = task.state
            task_result_state = task_state.result_state.value
            if task_result_state != "SUCCESS":
                failed_task = {
                    "task_key": task.task_key,
                    "run_id": task.run_id,
                    "result_state": task_result_state,
                    "state_message": task_state.state_message,
                }
                failed_tasks.append(failed_task)

        if failed_tasks:
            error_details = "\n".join(
                f"Result status of task_key:{task['task_key']}, run_id:{task['run_id']}, {task['result_state']}, {task['state_message']}."
                for task in failed_tasks
            )
            raise JobWorkspaceClientError(error_details)

    def trigger_workflow(
        self,
        job_id: str,
        params: object,
        python_params: list,
        wait_for_completion: bool = True,
        validate_task_success: bool = True,
    ) -> str:
        """
        Trigger a Databricks job workflow and optionally wait for its completion and validate task success.

        Args:
            job_id (str): The unique identifier of the Databricks job.
            params (object): Parameters to be passed to the job.
            wait_for_completion (bool, optional): Whether to wait for the job to complete (default: True).
            validate_task_success (bool, optional): Whether to validate the success of tasks in the job (default: True).

        Returns:
            str: The unique identifier of the triggered job run.

        Raises:
            JobWorkspaceClientError: If wait_for_completion is True and the job does not complete successfully.
        """
        w = WorkspaceClient()
        run_response = w.jobs.run_now(job_id=job_id, notebook_params=params, python_params=python_params)
        run_id = run_response.run_id

        if wait_for_completion:
            self.wait_for_job_completion(run_id)

            if validate_task_success:
                self.validate_task_success(run_id)

        return run_id

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Resources Function

# COMMAND ----------

def setup_database(database: str) -> None:
    """Method to setup mock database based on config

    Args:
        database (str): name of the database
    """
    spark.sql(f"""CREATE DATABASE IF NOT EXISTS {database};""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Teardown Resources Function

# COMMAND ----------

def teardown_table(database: str, table: str):
    """Method to drop table and physical files based on configs
    if table_location is not included in config, use Spark SQL to lookup for physical location.

    Args:
        spark (SparkSession): spark session
        configs (list): config input
    """
    # expected the table to be a managed table
    spark.sql(f"""DROP TABLE IF EXISTS {database}.{table}""")

# COMMAND ----------

def teardown_database(database: str, database_location: str = "") -> None:
    """Method to drop database and physical files based on configs
       if database_location is not included in config, use Spark SQL to lookup for physical location.

    Args:
        database (str): name of the dropping table's database
        database_location (str): location of the database
    """
    spark.sql(f"""DROP DATABASE IF EXISTS {database}""")

    if database_location:
        dbutils.fs.rm(f"{database_location}", True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Test Task

# COMMAND ----------

class TestTask:
    """
    Represents a test task for executing pipelines with different configurations and displaying results.

    This class facilitates the testing of pipelines with various configurations. It takes a dictionary of
    configurations, instantiates a `TaskInputValidation` object, and performs test parameter combination
    to execute pipelines with different input patterns. It also displays the results using the result object.

    Usage:
        task_configs = {
            "workflow_job_id": 123,
            "pipeline_class": MyPipeline,
            "result_class": MyResult,
            "dl_data_dt": "2023-08-15",
            "test_param": {"cluster": ['A', 'B'], "datasize": [100, 200]},
            # ... other configurations ...
        }
        test_task = TestTask(task_configs)
        test_task.execute()
    """

    def __init__(self, configs: dict):
        """
        Initializes a TestTask object with the provided configurations.

        Args:
            configs (dict): A dictionary containing configurations for the task.
        """
        self.configs = TaskInputValidation(**configs)
        self.configs.result_class.clear_class_attribute()

    def generate_test_param_combinations(self) -> list:
        """
        A method to create a cross product of elements in 'test_param'
        For example, for cluster ['A', 'B'] and datasize [100,200],
        the product would be each cluster paired with data size 100 and 200.

        Returns:
            list: A list of configuration dictionaries with different test parameter combinations.
        """

        if self.configs.test_param:

            test_param_values = self.configs.test_param
            test_param_keys = test_param_values.keys()

            param_combinations = list(itertools.product(*test_param_values.values()))
            task_inputs = []
            for combination in param_combinations:
                new_dict = self.configs.copy()
                new_dict.test_param = dict(zip(test_param_keys, combination))
                task_inputs.append(new_dict)
            return task_inputs
        else:
            return [self.configs]

    def display_results(self):
        """
        Displays the test results using the result object.
        """
        self.result_obj = self.configs.result_class(self.configs)
        self.result_obj.get_test_results_df().display()

    def execute(self):
        """
        Executes the test task by generating input combinations, executing pipelines, and displaying results.
        """
        self.task_inputs = self.generate_test_param_combinations()
        self.pipeline_objs = []

        # multiple executions for performanc test where there are multiple inputs patterns
        for task_input in self.task_inputs:
            pipeline_obj = self.configs.pipeline_class(task_input)
            pipeline_obj.execute()
            self.pipeline_objs.append(pipeline_obj)

        self.display_results()

# COMMAND ----------

# MAGIC %md
# MAGIC #Decorator Class

# COMMAND ----------

class TestResultDecorator(type):
    """
    Metaclass that decorates methods in a class to capture test results and handle errors.
    """

    def __new__(cls, name, bases, class_dict):
        """
        Creates a new class with decorated methods that capture test results and handle errors.

        Args:
            cls (type): The metaclass instance.
            name (str): The name of the new class.
            bases (tuple): The base classes of the new class.
            class_dict (dict): The dictionary containing class attributes.

        Returns:
            type: The newly created class with decorated methods.
        """
        for attr_name, attr_value in class_dict.items():
            if callable(attr_value):  # Check if the attribute is a method
                if attr_name != "__init__":
                    class_dict[attr_name] = get_test_results_dict(
                        try_except_error_handling(attr_value)
                    )

        return super().__new__(cls, name, bases, class_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC #ElapseTimeClass

# COMMAND ----------

class ElapseTime:
    """
    A utility class for measuring and recording the elapsed time of functions and tasks.

    This class provides methods to start and stop tracking the execution time of functions
    and tasks, and it stores the results for retrieval.

    Attributes:
        function_results (dict): A class-level dictionary to store function execution results.
                                Each function's data includes its name, start and end times,
                                and execution time.

    Note:
        - The `_start_point` and `_stop_point` methods are meant for internal use to manually track
          the execution time of functions.
        - The `measure_function` decorator can be applied to functions to automatically track their
          execution time.
        - The `get_function_results` method retrieves and clears the recorded function execution results.
    """

    function_results = {}

    @classmethod
    def _start_point(cls, function_nm: str):
        """
        Start tracking the execution time of a function or task.

        Args:
            function_nm (str): The name of the function or task.
        """

        # insert value to full data
        cls.function_results[function_nm] = {}
        cls.function_results[function_nm]["sub_process"] = function_nm
        cls.function_results[function_nm]["start_point"] = timeit.default_timer()
        cls.function_results[function_nm]["start_time"] = datetime.now()

    @classmethod
    def _stop_point(cls, function_nm: str):
        """
        Stop tracking the execution time of a function or task.

        Args:
            function_nm (str): The name of the function or task.
        """

        cls.function_results[function_nm]["end_point"] = timeit.default_timer()
        cls.function_results[function_nm]["end_time"] = datetime.now()
        execution_time = (
            cls.function_results[function_nm]["end_point"]
            - cls.function_results[function_nm]["start_point"]
        )
        cls.function_results[function_nm]["execution_duration"] = round(
            execution_time, 2
        )

        # remove unnessecary value
        del cls.function_results[function_nm]["start_point"]
        del cls.function_results[function_nm]["end_point"]

    @classmethod
    def get_function_results(cls):
        """
        Retrieve the recorded function execution results.

        Returns:
            list: A list of dictionaries containing execution results for each tracked function.
        """
        function_results = [dict_info for dict_info in cls.function_results.values()]
        cls.function_results = {}
        return function_results

    @classmethod
    def measure_function(cls, func):
        """
        Decorator for measuring the execution time of a function.

        Args:
            func (function): The function to be measured.

        Returns:
            function: The decorated function.
        """
        function_name = func.__name__

        def wrapper(*args, **kwargs):
            cls._start_point(function_name)
            func(*args, **kwargs)
            cls._stop_point(function_name)

        return wrapper

    def get_tasks_results(workflow_run_id: int):
        """
        Retrieve the execution results of tasks from a workflow run.

        Args:
            workflow_run_id (int): The ID of the workflow run.

        Returns:
            list: A list of dictionaries containing task execution results.

        """
        workflow_info = get_workflow_info(run_id=workflow_run_id)
        tasks_results = [
            {
                "workflow_name": workflow_info.run_name,
                "task_name": task.task_key,
                "start_time": unix_to_timestamp(task.start_time),
                "end_time": unix_to_timestamp(task.end_time),
                "setup_duration": task.setup_duration / 1000,
                "execution_duration": task.execution_duration / 1000,
            }
            for task in workflow_info.tasks
        ]
        return tasks_results
