import random
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def random_prediction_udf():
    return udf(lambda: random.randint(0, 1), IntegerType())
