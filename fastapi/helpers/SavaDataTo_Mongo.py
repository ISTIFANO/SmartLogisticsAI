from pymongo import MongoClient
import pandas as pd

MONGO_URI = "mongodb://localhost:27017"

def save_to_mongo(batch_df, batch_id):
    client = MongoClient(MONGO_URI)
    db = client.smartlogai
    col = db.streaming_insights

    records = batch_df.toPandas().to_dict("records")
    for r in records:
        r["window"] = str(r["window"])

    if records:
        col.insert_many(records)

    client.close()
    print(f" Mongo Saved Batch {batch_id}")
