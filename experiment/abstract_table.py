# Databricks notebook source
from abc import ABC
from abc import abstractmethod


class AbstractTable(ABC):
    """
    An abstract class for setup and teardown test tables
    """

    # naming: Table<tbl_name>
    def __init__(self, configs):
        """
        The __init__ method of 'AbstractTable' class
        Args:
            configs (TestPipelineConfigs): Configuration object containing pipeline settings.
        """

        # serves as a template, these parameters should be set in each table class
        database: str
        table: str

        # class instance variables

        self.database = database
        self.table = table

    @abstractmethod
    def setup(self):
        """
        Abstract method for setting up a table
        """
        pass

    def teardown(self):
        """Method to drop table and physical files based on configs
        if table_location is not included in config, use Spark SQL to lookup for physical location.

        Args:
            spark (SparkSession): spark session
            configs (list): config input
        """
        # note: expected the table to be a managed table
        spark.sql(f"""DROP TABLE IF EXISTS {self.database}.{self.table}""")
