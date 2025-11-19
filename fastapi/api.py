# fastapi_app.py
from fastapi import FastAPI, WebSocket
import asyncio
import random
import json
from datetime import datetime

app = FastAPI()

@app.websocket("/websocket")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        data = {
            "Days_for_shipment_scheduled": random.randint(1, 10),
            "Benefit_per_order": round(random.uniform(10, 200), 2),
            "Sales_per_customer": round(random.uniform(20, 500), 2),
            "Delivery_Status": random.choice(["Delivered", "Pending"]),
            "Customer_Country": random.choice(["USA", "France", "Germany"]),
            "Customer_City": random.choice(["New York", "Paris", "Berlin"]),
            "Order_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        await websocket.send_text(json.dumps(data))
        await asyncio.sleep(1) 
