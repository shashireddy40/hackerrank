from pyspark.sql import SparkSession, DataFrame, Window
from main.base import PySparkJobInterface
import pyspark.sql.functions as F


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        # TODO: put your code here
        ...

    def filter_medical(self, eligibility: DataFrame, medicals: DataFrame) -> DataFrame:
        # TODO: put your code here
        ...

    def generate_full_name(self, eligibility: DataFrame, medical: DataFrame) -> DataFrame:
        # TODO: put your code here
        ...

    def find_max_paid_member(self, medicals: DataFrame) -> str:
        # TODO: put your code here
        ...

    def find_total_paid_amount(self, medicals: DataFrame) -> int:
        # TODO: put your code here
        ...
