from pyspark.sql import SparkSession


def extract_data(spark):
    df = spark.read.options(header='true', inferSchema='true') \
              .csv("gs://us-east1-dev-cloud-composer-fb241977-bucket/data/retail_day.csv")
    return df

def transform_data(df, spark):
    df.createOrReplaceTempView("sales")
    df_highestPriceUnit = spark.sql("select * from sales where UnitPrice >= 3.0")
    return df_highestPriceUnit


def load_data(df):
    df.write.parquet("gs://us-east1-dev-cloud-composer-fb241977-bucket/data/highest_prices.parquet")


def main():
    spark = SparkSession \
    .builder \
    .appName("My PySpark code") \
    .getOrCreate()

    df_extract = extract_data(spark)
    df_transform = transform_data(df_extract, spark)
    load_data(df_transform)


if __name__ == '__main__':
    main()

