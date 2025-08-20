from datetime import datetime, timedelta
import os, json, requests, pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

RAW_PATH = "/opt/airflow/data/raw"
PROCESSED_PATH = "/opt/airflow/data/processed"
os.makedirs(RAW_PATH, exist_ok=True)
os.makedirs(PROCESSED_PATH, exist_ok=True)

LAT, LON = -38.9516, -68.0591  # Neuquén, AR

def extract():
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        f"&hourly=temperature_2m&timezone=auto"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    with open(os.path.join(RAW_PATH, "weather.json"), "w") as f:
        json.dump(r.json(), f)

def transform():
    with open(os.path.join(RAW_PATH, "weather.json")) as f:
        data = json.load(f)
    hours = data["hourly"]["time"]
    temps = data["hourly"]["temperature_2m"]
    df = pd.DataFrame({"time": pd.to_datetime(hours), "temperature": temps})
    df["date"] = df["time"].dt.date
    daily = df.groupby("date", as_index=False)["temperature"].mean().rename(columns={"temperature": "avg_temp_c"})
    daily.to_csv(os.path.join(PROCESSED_PATH, "daily_temps.csv"), index=False)

def load_mysql():
    conn = BaseHook.get_connection("mysql_etl")
    engine = create_engine(conn.get_uri())
    df = pd.read_csv(os.path.join(PROCESSED_PATH, "daily_temps.csv"))
    df.to_sql("weather_daily", con=engine, if_exists="append", index=False)

default_args = {"owner": "nico", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="etl_weather_to_mysql",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["portfolio","etl","mysql","airflow"],
    description="ETL: Open-Meteo -> transform -> MySQL"
) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load_mysql", python_callable=load_mysql)
    t1 >> t2 >> t3
