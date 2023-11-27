# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from abc import ABC, abstractmethod

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Abstract Pipeline

# COMMAND ----------

class AbstractTestPipeline(ABC):
    """
    Represents an end-to-end test pipeline for validating test area. This pipeline sets up resources, runs tests, saves results,
    and then tears down resources after execution.
    """

    def __init__(self, configs) -> None:
        """
        Args:
            configs (TestPipelineConfigs): Configuration object containing pipeline settings.
        """
        self.configs = configs
        self.test_results = []

    @abstractmethod
    def setup_resource(self) -> None:
        """
        An abstract method for setting up resources for each test case
        """
        pass

    def trigger_workflow(self, validate_task_success: bool = True) -> None:
        """
        Method that calls trigger workflow according to'workflow_job_id' and use 'workflow_input_param' as inputs.

        Args:
            validate_task_success (bool, optional): Validate if task success when set to True. Defaults to True.

        Returns:
            int: the workflow_run_id of the workflow run
        """
        job_workspace_client_obj = JobWorkspaceClient()
        self.workflow_run_id = job_workspace_client_obj.trigger_workflow(
            job_id=self.configs.workflow_job_id,
            params=self.configs.workflow_input_param,
            python_params=self.configs.workflow_python_param, 
            validate_task_success=validate_task_success,
        )

    def update_test_results(
        self,
        test_results: list,
        test_param: dict = None,
        skip_workflow_info_flag: bool = False,
    ) -> None:
        """
        For each E2E test's assertion result or performance test workflow's task result,
        update each result dictionary in test result list to contain workflow's run information.

        Args:
            test_results (list): a list of test results from assertions or workflow's tasks
            test_param (dict): a dictionary of test case's parameters
            skip_workflow_info_flag (bool)(optional): Flag to define whether skip adding workflow_info or not, default value as False

        Returns:
            list: list of updated test results
        """
        if skip_workflow_info_flag is False:
            workflow_info = get_workflow_info(self.workflow_run_id)
            # for single task job run, run_duration will be 'None'
            if workflow_info.run_duration is None:
                workflow_execution_duration = workflow_info.execution_duration
            else:
                workflow_execution_duration = workflow_info.run_duration
            # create an update dict for each test result's row
            update_dict = {
                "workflow_name": workflow_info.run_name,
                "workflow_start_time": unix_to_timestamp(workflow_info.start_time),
                "workflow_end_time": unix_to_timestamp(workflow_info.end_time),
                "workflow_setup_duration": round(
                    workflow_info.setup_duration / 1000, 2
                ),
                "workflow_execution_duration": round(
                    workflow_execution_duration / 1000, 2
                ),
                "test_param": test_param,
            }
        elif skip_workflow_info_flag is True:
            # create an update dict for each test result's row
            update_dict = {"workflow_name": "skip", "test_param": test_param}
        else:
            raise ValueError("Detect skip_workflow_info_flag is not True or False")

        # update results from each assertion case
        for item in test_results:
            item.update(update_dict)
            self.test_results.append(item)

    @abstractmethod
    def run_test(self, *args, **kwargs) -> list:
        """
        An abstract method to orchestrate the assertion runs for E2E test and
        get the workflow's tasks and subprocesses information for performance test.
        """
        pass

    @abstractmethod
    def save_results(self, *args, **kwargs) -> None:
        """
        An abstract method for saving the test results.
        """
        pass

    @abstractmethod
    def teardown_resource(self):
        """
        An abstract method for tearing up resources for each test case, including
        tearing down outputs from the workflow run.
        """
        pass

    @abstractmethod
    def execute(self) -> None:
        """
        An abstract method for orchestrating the test case pipeline methods.
        """
        pass
