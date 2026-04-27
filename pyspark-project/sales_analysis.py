from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder     .appName("SalesAnalysis")     .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("/tmp/airflow_data/output.csv", header=True, inferSchema=True)
print(f"Total records: {df.count()}")
df.printSchema()

# Sales by region
print("\n=== Sales by Region ===")
df.groupBy("region")   .agg(
    F.count("*").alias("total_deals"),
    F.round(F.sum("sales"), 2).alias("total_sales"),
    F.round(F.avg("calls"), 2).alias("avg_calls")
  )   .orderBy(F.desc("total_sales"))   .show()

# Top salesperson by sales
print("\n=== Top Salespersons ===")
df.groupBy("salesperson")   .agg(F.round(F.sum("sales"), 2).alias("total_sales"))   .orderBy(F.desc("total_sales"))   .show(10)

# Window function - rank salesperson within region
print("\n=== Salesperson Rank by Region ===")
window = Window.partitionBy("region").orderBy(F.desc("sales"))
df.withColumn("rank", F.rank().over(window))   .filter(F.col("rank") == 1)   .select("region", "salesperson", "sales", "rank")   .show()

import os
os.makedirs("/tmp/pyspark_output", exist_ok=True)
df.write.mode("overwrite").parquet("/tmp/pyspark_output/sales_analysis")
print("\nOutput saved!")
spark.stop()
