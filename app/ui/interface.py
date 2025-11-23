# interface.py
import streamlit as st
import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client.smartlogai
collection = db.streaming_insights

data = list(collection.find())
df = pd.DataFrame(data)


for col in ["Customer_Country", "Customer_City", "total_orders", "avg_sale", "total_benefit", "risk_score"]:
    if col not in df.columns:
        df[col] = None


st.title(" SmartLogistics Streaming Dashboard")

country_options = df["Customer_Country"].dropna().unique()
country_filter = st.selectbox("Select Country", options=country_options)

city_options = df[df["Customer_Country"] == country_filter]["Customer_City"].dropna().unique()
if len(city_options) > 0:
    city_filter = st.selectbox("Select City", options=city_options)
else:
    city_filter = None

filtered_df = df[df["Customer_Country"] == country_filter]
if city_filter:
    filtered_df = filtered_df[filtered_df["Customer_City"] == city_filter]


st.subheader(" Key Metrics")
st.metric("Total Orders", filtered_df["total_orders"].sum())
st.metric("Average Sale", filtered_df["avg_sale"].mean())
st.metric("Total Benefit", filtered_df["total_benefit"].sum())
st.metric("Risk Score", filtered_df["risk_score"].mean())

st.subheader(" Charts")
st.bar_chart(filtered_df.set_index("Customer_City")[["total_orders", "avg_sale", "total_benefit", "risk_score"]])
