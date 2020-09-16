import unittest
import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join('example-airflow')))
sys.path.append(os.path.abspath(os.path.join('../')))

from sparkfiles.transformation import transform_data
from pyspark.sql import SparkSession


class PySparkTest(unittest.TestCase):
    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)


    @classmethod
    def create_testing_pyspark_session(cls):
        return SparkSession \
                    .builder \
                    .master('local[*]') \
                    .appName("my-local-testing-pyspark-context") \
                    .getOrCreate()


    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()
        cls.test_data_path = "gs://us-east1-dev-cloud-composer-fb241977-bucket/data/retail_day.csv"
        cls.df = cls.spark.read.options(header='true', inferSchema='true') \
                    .csv(cls.test_data_path)
        cls.df_exepcted = transform_data(cls.df, cls.spark)


    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class PySparkETLTest(PySparkTest):
    def test_transform_data(self):    
        self.expected_cols = len(self.df.columns)
        self.actual_cols = len(self.df_exepcted.columns)
        self.assertEqual(self.expected_cols, self.actual_cols)


if __name__ == '__main__':
    unittest.main()


