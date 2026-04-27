import json
import time
import random
import boto3
import psycopg2
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# ============ STEP 1: KAFKA PRODUCER ============
def produce_events(n=100):
    print("\n=== STEP 1: Producing events to Kafka ===")
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    regions = ["North", "South", "East", "West"]
    salespersons = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    products = ["Software", "Hardware", "Services"]

    events = []
    for i in range(n):
        event = {
            "event_id": i,
            "timestamp": datetime.now().isoformat(),
            "salesperson": random.choice(salespersons),
            "region": random.choice(regions),
            "amount": round(random.uniform(100, 5000), 2),
            "product": random.choice(products)
        }
        producer.send("sales-events", value=event)
        events.append(event)
    producer.flush()
    print(f"Produced {n} events to Kafka")
    return events

# ============ STEP 2: SAVE TO S3 ============
def save_to_s3(events):
    print("\n=== STEP 2: Saving to AWS S3 ===")
    df = pd.DataFrame(events)
    os.makedirs("/tmp/pipeline", exist_ok=True)
    df.to_parquet("/tmp/pipeline/raw_events.parquet", index=False)
    s3 = boto3.client("s3", region_name="us-east-1")
    date_str = datetime.now().strftime("%Y-%m-%d")
    s3.upload_file("/tmp/pipeline/raw_events.parquet", 
                   "rohit-airflow-pipeline-2024", 
                   f"pipeline/{date_str}/raw_events.parquet")
    print(f"Saved {len(events)} events to S3")

# ============ STEP 3: PYSPARK TRANSFORM ============
def transform_with_spark():
    print("\n=== STEP 3: Transforming with PySpark ===")
    spark = SparkSession.builder.appName("E2EPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet("/tmp/pipeline/raw_events.parquet")
    transformed = df.groupBy("region", "product")         .agg(
            F.count("*").alias("total_deals"),
            F.round(F.sum("amount"), 2).alias("total_revenue"),
            F.round(F.avg("amount"), 2).alias("avg_deal_size")
        )         .orderBy(F.desc("total_revenue"))
    transformed.show()
    transformed.write.mode("overwrite").parquet("/tmp/pipeline/transformed")
    spark.stop()
    print("PySpark transformation complete!")
    return pd.read_parquet("/tmp/pipeline/transformed")

# ============ STEP 4: LOAD TO POSTGRESQL ============
def load_to_postgres(df):
    print("\n=== STEP 4: Loading to PostgreSQL ===")
    conn = psycopg2.connect(
        host="localhost", database="salesdb",
        user="rohit", password="rohit123"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_summary (
            region VARCHAR(50),
            product VARCHAR(50),
            total_deals INTEGER,
            total_revenue FLOAT,
            avg_deal_size FLOAT,
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("TRUNCATE TABLE sales_summary")
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO sales_summary (region, product, total_deals, total_revenue, avg_deal_size) VALUES (%s, %s, %s, %s, %s)",
            (row["region"], row["product"], int(row["total_deals"]), float(row["total_revenue"]), float(row["avg_deal_size"]))
        )
    conn.commit()
    print(f"Loaded {len(df)} rows to PostgreSQL")

    # Final report
    print("\n=== FINAL REPORT FROM POSTGRESQL ===")
    cur.execute("SELECT region, product, total_deals, total_revenue FROM sales_summary ORDER BY total_revenue DESC")
    rows = cur.fetchall()
    print(f"{'Region':<10} {'Product':<12} {'Deals':<8} {'Revenue':<12}")
    print("-" * 45)
    for row in rows:
        print(f"{row[0]:<10} {row[1]:<12} {row[2]:<8} ${row[3]:<12}")
    cur.close()
    conn.close()

# ============ RUN PIPELINE ============
if __name__ == "__main__":
    print("Starting End-to-End Data Pipeline")
    print("=" * 50)
    events = produce_events(100)
    save_to_s3(events)
    df = transform_with_spark()
    load_to_postgres(df)
    print("\n✅ Pipeline complete!")
