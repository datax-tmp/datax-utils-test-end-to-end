# Databricks notebook source
# MAGIC %md
# MAGIC #Data Correctness

# COMMAND ----------

# import: pyspark
from pyspark.sql import DataFrame

# import: external
from feast.feature_store import FeatureStore


class TestFeastMetadata(metaclass=TestResultDecorator):
    """
    A class containing methods for validating Feast feature store component.
    """

    def check_feature_view_metadata(
        fs: FeatureStore, df: DataFrame, feature_view_name: str, **kwargs
    ) -> str:
        """
        Checks if the metadata of a feature view matches the expected columns in a target DataFrame.

        Args:
            fs (FeatureStore): The feature store object.
            df (DataFrame): The target DataFrame with expected columns.
            feature_view_name (str): The name of the feature view to check metadata for.

        Raises:
            AssertionError: If the metadata columns of the feature view do not match the expected columns.
        """
        expected_columns = df.columns

        # Get FeatureView object from given `feature_view_name`
        feature_view_obj = fs.get_feature_view(feature_view_name)

        # Fetch all columns list from FeatureView
        fetch_columns_list = []
        fetch_columns_list.extend(
            [entity_column.name for entity_column in feature_view_obj.entity_columns]
        )
        fetch_columns_list.extend([feature.name for feature in feature_view_obj.features])

        # Assert that column in FeatureView metadata is matched with expected
        assert set(expected_columns) == set(
            fetch_columns_list
        ), "Column metadata is mismatched"
        return "Success"

    def check_feature_service_metatdata(
        fs: FeatureStore, df: DataFrame, feature_service_name: str, **kwargs
    ) -> str:
        """
        Checks if the metadata of a feature service matches the expected columns in a target DataFrame.

        Args:
            fs (FeatureStore): The feature store object.
            df (DataFrame): The target DataFrame with expected columns.
            feature_service_name (str): The name of the feature service to check metadata for.

        Raises:
            AssertionError: If the metadata columns of the feature service do not match the expected columns.
        """
        expected_columns = df.columns

        # Get a list of FeatureViewProjection
        list_feature_view_proj = fs.get_feature_service(
            feature_service_name
        ).feature_view_projections

        # Iterate each FeatureViewProjection, fetech all columns list from all FeatureViews
        fetch_columns_list = []
        for each_feature_view_proj in list_feature_view_proj:
            # Get FeatureView from name
            each_feature_view_obj = fs.get_feature_view(name=each_feature_view_proj.name)

            # Fetch all columns list from FeatureView
            fetch_columns_list.extend(
                [
                    entity_column.name
                    for entity_column in each_feature_view_obj.entity_columns
                ]
            )
            fetch_columns_list.extend(
                [feature.name for feature in each_feature_view_obj.features]
            )

        # Assert that column in FeatureService metadata is matched with expected
        assert set(expected_columns) == set(
            fetch_columns_list
        ), "Column metadata is mismatched"
        return "Success"
