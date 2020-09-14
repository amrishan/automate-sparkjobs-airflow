from pyspark.sql import SparkSession
from airflow import models

spark = SparkSession \
    .builder \
    .appName("My PySpark code") \
    .getOrCreate()

df = spark.read.options(header='true', inferSchema='true').csv("{}data/retail_day.csv".format(models.Variable.get('bucket')))
df.printSchema()

df.createOrReplaceTempView("sales")
highestPriceUnitDF = spark.sql("select * from sales where UnitPrice >= 3.0")

highestPriceUnitDF.write.parquet("{}data/highest_prices.parquet".format(models.Variable.get('bucket')))