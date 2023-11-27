# Databricks notebook source
# MAGIC %md
# MAGIC # Workflow Completeness

# COMMAND ----------

from datax.engine.job_launcher.api.api_client import ApiClient

# COMMAND ----------

class WorkflowCompleteness(metaclass=TestResultDecorator):
    """
    A class containing methods for validating the completeness of tasks within a workflow.
    """

    def check_all_tasks_complete(workflow_run_id: int, **kwargs) -> str:
        """
        Validates whether all tasks within a workflow run have completed successfully.

        Args:
            workflow_run_id (int): The ID of the workflow run to be checked.

        Returns:
            str: "Success" if all tasks have completed successfully, otherwise raises an assertion error.
        """
        api_client = ApiClient()
        workflow_info = api_client.perform_query(
            request_type="GET",
            end_point="jobs/runs/get",
            param={"run_id": workflow_run_id},
        ).json()

        not_success_task = [
            {"task_name": task["task_key"], "results": task["state"]["result_state"]}
            for task in workflow_info["tasks"]
            if task["state"]["result_state"] != "SUCCESS"
        ]

        assert not not_success_task, f"Found not success task : {not_success_task}"

        return "Success"
