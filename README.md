# Predictive Maintenance Real-Time Dashboard

This project is a real-time predictive maintenance system that ingests sensor data from a Kafka topic, processes it with Apache Spark, stores results in MongoDB, and visualizes predictions in a modern React dashboard.

## Features
- **Real-time data ingestion** from Kafka
- **Streaming and batch processing** with PySpark
- **Prediction results stored in MongoDB**
- **FastAPI backend** to serve prediction data
- **React dashboard** for visualization:
  - Key metrics (total batches, failure/normal counts, failure rate)
  - Pie chart for prediction distribution
  - Line chart showing hourly trends for normal and failure predictions
  - Table of recent predictions
  - Filters for date, machine type, and prediction label
  - Auto-refresh and manual refresh

## Project Structure
```
├── dashboard_api.py         # FastAPI backend
├── spApp.py                 # Spark streaming app (Kafka → MongoDB)
├── producer.py              # Kafka data producer
├── machine_failure_model.pkl# Trained ML model
├── my-app/                  # React frontend (dashboard)
│   └── src/
│       └── dashboard.jsx    # Main dashboard component
├── ...
```

## Getting Started

### Prerequisites
- Python 3.11+
- Node.js & npm
- Apache Kafka
- MongoDB
- Apache Spark (with PySpark)

### 1. Start MongoDB and Kafka
- Ensure MongoDB and Kafka are running locally.

### 2. Start the Spark Streaming App
```powershell
python spApp.py
```

### 3. Start the FastAPI Backend
```powershell
python dashboard_api.py
```

### 4. Start the React Dashboard
```powershell
cd my-app
npm install
npm start
```
The dashboard will be available at `http://localhost:3000`.

## Usage
- The dashboard auto-refreshes every 5 seconds.
- Use filters to view predictions by date, machine type, or label.
- The line chart shows hourly trends for both normal and failure predictions.

## Notes
- Data ingestion and prediction logic are handled in `spApp.py` using Spark and your ML model.
- The backend (`dashboard_api.py`) serves data from MongoDB to the React frontend.
- No Hadoop/HDFS is used; all processing is via Spark, Kafka, and MongoDB.

## License
MIT
