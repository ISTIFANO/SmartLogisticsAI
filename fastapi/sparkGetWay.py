# sparkGetWay.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType,IntegerType

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
HADOOP_PATH = os.path.join(PROJECT_DIR, "hadoop")

os.environ["HADOOP_HOME"] = HADOOP_PATH
os.environ["hadoop.home.dir"] = HADOOP_PATH
os.environ["PATH"] = os.path.join(HADOOP_PATH, "bin") + ";" + os.environ["PATH"]

print("HADOOP_HOME =", os.environ["HADOOP_HOME"])
print("PATH =", os.environ["PATH"])

winutils_path = os.path.join(HADOOP_PATH, "bin", "winutils.exe")
print("winutils exists:", os.path.exists(winutils_path))
print("winutils path:", winutils_path)

def start_streaming(spark):
    schema = StructType([
        StructField("Days_for_shipment_scheduled", IntegerType(), True),
        StructField("Benefit_per_order", DoubleType(), True),
        StructField("Sales_per_customer", DoubleType(), True),
        StructField("Delivery_Status", StringType(), True),
        StructField("Customer_Country", StringType(), True),
        StructField("Customer_City", StringType(), True),
        StructField("Order_date", StringType(), True)
    ])

    df = spark.readStream.format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    parsed = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    query = parsed.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SmartLogisticsStream") \
        .master("local[*]") \
        .getOrCreate()

    start_streaming(spark)
