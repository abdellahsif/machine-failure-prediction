from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from bson import ObjectId
import os

app = FastAPI()

# Allow CORS for local frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)
db = client["predictive_maintenance"]
collection = db["predictions"]

def serialize(doc):
    doc["_id"] = str(doc["_id"])
    return doc

@app.get("/predictions")
def get_predictions():
    # Get the latest 200 predictions
    docs = list(collection.find().sort([("_id", -1)]).limit(200))
    return [serialize(doc) for doc in docs]