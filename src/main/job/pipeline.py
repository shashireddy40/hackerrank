from pyspark.sql import SparkSession, DataFrame, Window
from main.base import PySparkJobInterface
import pyspark.sql.functions as F


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        # TODO: put your code here
        spark = SparkSession.builder.master("local").appName("Data Cleaning").getOrCreate()

    def filter_medical(self, eligibility: DataFrame, medicals: DataFrame) -> DataFrame:
        # TODO: put your code here
        #medicaldf = spark.read.format("csv").option("header",True).schema(medical).load("data/medical.csv")
        df = spark.read_csv("data/medical.csv", medical)
        return df.show()


    def generate_full_name(self, eligibility: DataFrame, medical: DataFrame) -> DataFrame:
        # TODO: put your code here
        ...

    def find_max_paid_member(self, medicals: DataFrame) -> str:
        # TODO: put your code here
        ...

    def find_total_paid_amount(self, medicals: DataFrame) -> int:
        # TODO: put your code here
        ...
