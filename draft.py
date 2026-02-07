from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('app').getOrCreate()
df = spark.read.csv("/Users/suksientie/Downloads/master", header=True, inferSchema=True)

assert(df.distinct().count() == df.count())