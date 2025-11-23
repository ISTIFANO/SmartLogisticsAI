from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, window, count, avg, sum as spark_sum, rand, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from helpers.Model import random_prediction_udf
from helpers.SavaDataTo_Pg import init_postgres_table, save_to_postgres
from helpers.SavaDataTo_Mongo import save_to_mongo
import os
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
HADOOP_PATH = os.path.join(PROJECT_DIR, "hadoop")
os.environ["PYSPARK_PYTHON"] = r"C:\Users\aamir\AppData\Local\Programs\Python\Python313\python.exe"

os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\aamir\AppData\Local\Programs\Python\Python313\python.exe"


os.environ["HADOOP_HOME"] = HADOOP_PATH
os.environ["hadoop.home.dir"] = HADOOP_PATH
os.environ["PATH"] = os.path.join(HADOOP_PATH, "bin") + ";" + os.environ["PATH"]

print("HADOOP_HOME =", os.environ["HADOOP_HOME"])
print("PATH =", os.environ["PATH"])

def start_streaming():
    init_postgres_table()

    spark = (
        SparkSession.builder
        .appName("SmartLogisticsStream")
        .master("local[*]")
        .getOrCreate()
    )

    schema = StructType([
        StructField("Days_for_shipment_scheduled", IntegerType(), True),
        StructField("Benefit_per_order", DoubleType(), True),
        StructField("Sales_per_customer", DoubleType(), True),
        StructField("Delivery_Status", StringType(), True),
        StructField("Customer_Country", StringType(), True),
        StructField("Customer_City", StringType(), True),
        StructField("Order_date", StringType(), True)
    ])

    df = (
        spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
    )
    json_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    predictions = json_df.withColumn("prediction", when(rand() > 0.5, 1).otherwise(0))
    predictions_ts = predictions.withColumn("prediction_timestamp", current_timestamp())

    windowed = (
        predictions_ts
        .withWatermark("prediction_timestamp", "10 minutes")
        .groupBy(
            window("prediction_timestamp", "5 minutes"),
            "Customer_Country"
        )
        .agg(
            count("*").alias("total_orders"),
            avg("Sales_per_customer").alias("avg_sale"),
            spark_sum("Benefit_per_order").alias("total_benefit"),
            avg("prediction").alias("risk_score")
        )
    )

    q1 = (
        predictions_ts.writeStream
        .foreachBatch(save_to_postgres)
        .outputMode("append")
        .start()
    )

    q2 = (
        windowed.writeStream
        .foreachBatch(save_to_mongo)
        .outputMode("update")
        .start()
    )

    q1.awaitTermination()
    q2.awaitTermination()
