from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("Days_for_shipment_scheduled", IntegerType(), True),
    StructField("Benefit_per_order", DoubleType(), True),
    StructField("Sales_per_customer", DoubleType(), True),
    StructField("Delivery_Status", StringType(), True),
    StructField("Customer_Country", StringType(), True),
    StructField("Customer_City", StringType(), True),
    StructField("Order_date", StringType(), True)
])

allcol = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

df_stream = allcol.select(from_json(col("value"), schema).alias("data")).select("data.*")


print(df_stream)