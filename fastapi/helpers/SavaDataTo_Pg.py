import psycopg2
import pandas as pd

POSTGRES_URI = "postgresql://postgres:postgres@localhost:5432/logisticDB"

def init_postgres_table():
    conn = psycopg2.connect(POSTGRES_URI)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS predictions_stream (
            id SERIAL PRIMARY KEY,
            days_for_shipment INTEGER,
            benefit_per_order DOUBLE PRECISION,
            sales_per_customer DOUBLE PRECISION,
            delivery_status VARCHAR(50),
            customer_country VARCHAR(50),
            customer_city VARCHAR(50),
            order_date VARCHAR(100),
            prediction INTEGER,
            prediction_timestamp TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def save_to_postgres(batch_df, batch_id):
    pdf = batch_df.toPandas()
    conn = psycopg2.connect(POSTGRES_URI)
    cur = conn.cursor()
    for _, r in pdf.iterrows():
        cur.execute("""
            INSERT INTO predictions_stream (
                days_for_shipment,
                benefit_per_order,
                sales_per_customer,
                delivery_status,
                customer_country,
                customer_city,
                order_date,
                prediction,
                prediction_timestamp
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            r["Days_for_shipment_scheduled"],
            r["Benefit_per_order"],
            r["Sales_per_customer"],
            r["Delivery_Status"],
            r["Customer_Country"],
            r["Customer_City"],
            r["Order_date"],
            r["prediction"],
            r["prediction_timestamp"]
        ))
    conn.commit()
    cur.close()
    conn.close()
    print(f" PostgreSQL Saved Batch {batch_id}")
