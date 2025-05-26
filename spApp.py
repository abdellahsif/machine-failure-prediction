from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, IntegerType, StringType
import joblib
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# MongoDB setup (adjust if needed)
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["predictive_maintenance"]
mongo_collection = mongo_db["predictions"]

print("Starting predictive maintenance application...")

# Load the trained model
print("Loading trained model...")
model = joblib.load("machine_failure_model.pkl")
print("Model loaded successfully")

# Define schema for incoming JSON
print("Defining schema...")
schema = StructType() \
    .add("UDI", IntegerType()) \
    .add("Product ID", StringType()) \
    .add("Type", IntegerType()) \
    .add("Air temperature [K]", DoubleType()) \
    .add("Process temperature [K]", DoubleType()) \
    .add("Rotational speed [rpm]", DoubleType()) \
    .add("Torque [Nm]", DoubleType()) \
    .add("Tool wear [min]", DoubleType()) \
    .add("Machine failure", IntegerType()) \
    .add("TWF", IntegerType()) \
    .add("HDF", IntegerType()) \
    .add("PWF", IntegerType()) \
    .add("OSF", IntegerType()) \
    .add("RNF", IntegerType())
print("Schema defined")

# Create Spark session
print("Creating Spark session...")
spark = SparkSession.builder \
    .appName("PredictiveMaintenance") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()
print("Spark session created")

# Read from Kafka
print("Setting up Kafka stream reader...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data") \
    .load()
print("Kafka stream reader configured")

# Parse JSON messages
print("Setting up JSON parsing...")
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")
print("JSON parsing configured")

# Batch processing function
def predict_from_batch(batch_df, batch_id):
    print(f"Batch {batch_id} has {batch_df.count()} rows")
    print(f"Processing batch {batch_id}")
    if not batch_df.rdd.isEmpty():
        print(f"Batch {batch_id} has data, converting to pandas")

        pdf = batch_df.select(
            "Type",
            "Air temperature [K]",
            "Process temperature [K]",
            "Rotational speed [rpm]",
            "Torque [Nm]",
            "Tool wear [min]"
        ).toPandas()

        features_pdf = pdf.fillna({
            "Type": 2,
            "Air temperature [K]": 300,
            "Process temperature [K]": 310,
            "Rotational speed [rpm]": 1500,
            "Torque [Nm]": 40,
            "Tool wear [min]": 0
        })

        predictions = model.predict(features_pdf)
        probabilities = model.predict_proba(features_pdf)[:, 1]

        pdf['Prediction'] = predictions
        pdf['Failure Probability'] = probabilities
        pdf['Alert'] = pdf['Failure Probability'] > 0.6
        pdf['timestamp'] = datetime.utcnow().isoformat()

        # Save to MongoDB
        records = pdf.to_dict(orient='records')
        mongo_collection.insert_many(records)

        # Optional: print output
        print(f"\n===== Batch {batch_id} =====")
        type_reverse_map = {0: 'H', 1: 'L', 2: 'M'}
        for row in records:
            display_type = type_reverse_map.get(row['Type'], str(row['Type']))
            status = "🔴 ALERT!" if row['Prediction'] == 1 else "🟢 NORMAL"
            confidence = f"{row['Failure Probability']:.2%}"
            print(f"Type: {display_type} | Air Temp: {row['Air temperature [K]']:.1f} | "
                  f"Process Temp: {row['Process temperature [K]']:.1f} | "
                  f"Speed: {row['Rotational speed [rpm]']:.1f} | "
                  f"Torque: {row['Torque [Nm]']:.1f} | "
                  f"Tool wear: {row['Tool wear [min]']:.1f} | "
                  f"{status} | Confidence in prediction: {confidence}")
    else:
        print(f"Batch {batch_id} is empty, skipping")

# Start streaming query
print("Starting streaming query...")
query = json_df.writeStream \
    .foreachBatch(predict_from_batch) \
    .start()
print("Streaming query started")

print("Awaiting termination...")
query.awaitTermination()
