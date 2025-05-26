from kafka import KafkaProducer
import pandas as pd
import json
import time

# Load the CSV data
df = pd.read_csv('new_input.csv')

# Encode 'Type' as in training
type_map = {'H': 0, 'L': 1, 'M': 2}
if 'Type' in df.columns:
    df['Type'] = df['Type'].map(type_map)

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=5
)

topic_name = 'sensor-data'

print(f"Starting to stream {len(df)} records from new_input.csv")

for _, row in df.iterrows():
    message = row.to_dict()

    try:
        producer.send(topic_name, value=message)
    except Exception as e:
        print(f"Error sending message: {e}")

    print(f"Sent: {message}")
    time.sleep(5)  # Adjust delay as needed for your streaming speed

producer.flush()
producer.close()
print("Finished streaming data.")