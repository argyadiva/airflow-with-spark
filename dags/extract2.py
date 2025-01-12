import os
import pymongo
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("PySpark Example") \
        .getOrCreate()

    # Example: Read data
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # Transform data
    df = df.withColumn("AgeInTenYears", df["Age"] + 10)

    # Write output
    df.show()
    df.write.csv("/opt/airflow/dags/transformed_data", header=True)

if __name__ == "__main__":
    main()
