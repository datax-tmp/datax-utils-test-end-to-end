# Databricks notebook source
class ManualFile(metaclass=TestResultDecorator):
    """
    A class containing methods for validating data quality outputs' correctness

    """
    
    def check_file_not_exists(
        file_path: str, **kwargs
    ) -> str:
        """
        Check if the file in the specified path does not exists, used for checking if the file 
        in the landing zone has been moved to the archive path.

        Parameters:
            file_path (str): path to the file to be checked

        Returns:
            str: A success message if the check passes.

        Raises:
            AssertionError: If the file in the path exists and can be listed by dbutils.fs.ls
        """

        try:
            dbutils.fs.ls(file_path)
        except Exception as e:
            # Search for 'java.io.FileNotFoundException' in the error message
            # Because dbutils does not use the 'FileNotFound' error.
            if 'java.io.FileNotFoundException' in str(e):
                return "Success"
            else:
                raise e
        else:
            raise ValueError(f"The file in the landing zone path: {file_path} exists")

    def check_file_exists(
        file_path: str, **kwargs
    ) -> str:
        """
        Check if the file in the specified path exists, used for checking the archived file.

        Parameters:
            file_path (str): path to the file to be checked

        Returns:
            str: A success message if the check passes.

        Raises:
            AssertionError: If the file in the path does not exist
        """
        listed_files_info = dbutils.fs.ls(file_path)
        found_files = [file_info.name for file_info in listed_files_info]
            
        assert len(found_files) == 1, f"The number of files in the archive directory is not correct, files listed: {found_files}"

        return "Success"

# COMMAND ----------


