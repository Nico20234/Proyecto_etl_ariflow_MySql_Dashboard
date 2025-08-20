from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import mysql.connector

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(
    'csv_to_mysql',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def load_csv_to_mysql():
    # Leé CSV de ejemplo
    df = pd.read_csv('/opt/airflow/data/sample.csv')  # poné tu CSV en ./data/
    
    # Conexión a MySQL
    conn = mysql.connector.connect(
        host='mysql',
        user='airflow',
        password='airflow',
        database='weather',
        port=3306
    )
    cursor = conn.cursor()
    
    # Crear tabla si no existe
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(50),
            temperature FLOAT,
            humidity INT
        )
    """)
    
    # Insertar datos
    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO weather_data (city, temperature, humidity) VALUES (%s, %s, %s)",
            (row['city'], row['temperature'], row['humidity'])
        )
    conn.commit()
    cursor.close()
    conn.close()

load_task = PythonOperator(
    task_id='load_csv_to_mysql',
    python_callable=load_csv_to_mysql,
    dag=dag
)

