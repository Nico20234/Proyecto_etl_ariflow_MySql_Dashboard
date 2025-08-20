from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

default_args = {
    'owner': 'Nico',
    'start_date': datetime(2025, 8, 20),
}

with DAG(
    'mysql_test_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['test', 'mysql']
) as dag:

    # Crear tabla de prueba
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='mysql_etl',
        sql="""
        CREATE TABLE IF NOT EXISTS weather_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            city VARCHAR(50),
            temperature FLOAT,
            record_date DATE
        );
        """
    )

    # Insertar datos de prueba
    insert_data = MySqlOperator(
        task_id='insert_data',
        mysql_conn_id='mysql_etl',
        sql="""
        INSERT INTO weather_data (city, temperature, record_date) VALUES
        ('Buenos Aires', 20.5, '2025-08-20'),
        ('Córdoba', 18.2, '2025-08-20'),
        ('Rosario', 19.1, '2025-08-20');
        """
    )

    create_table >> insert_data

