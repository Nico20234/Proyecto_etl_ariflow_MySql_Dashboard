from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlalchemy

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'weather_ingest_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Función para cargar CSV a MySQL
def load_csv_to_mysql():
    # Leer CSV
    df = pd.read_csv('/opt/airflow/data/weather.csv')
    
    # Conexión a MySQL
    engine = sqlalchemy.create_engine('mysql+mysqlconnector://airflow:airflow@mysql:3306/weather')
    
    # Insertar datos en la tabla 'weather', si no existe la crea
    df.to_sql('weather', con=engine, if_exists='replace', index=False)
    print("Datos insertados correctamente en MySQL.")

# Tarea de PythonOperator
ingest_task = PythonOperator(
    task_id='ingest_weather_csv',
    python_callable=load_csv_to_mysql,
    dag=dag
)

