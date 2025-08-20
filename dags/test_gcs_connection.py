from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.dates import days_ago

# Nombre del bucket a probar
BUCKET_NAME = "tu-bucket-de-prueba"

with DAG(
    dag_id="test_gcs_connection",
    schedule_interval=None,  # Se ejecuta manualmente
    start_date=days_ago(1),
    catchup=False,
    tags=["test", "gcp"],
) as dag:

    list_files = GCSListObjectsOperator(
        task_id="list_gcs_files",
        bucket=BUCKET_NAME,
        gcp_conn_id="gcp_default",  # usamos la conexión creada antes
    )

    list_files

