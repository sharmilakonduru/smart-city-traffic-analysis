from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from google.cloud import bigquery

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='traffic_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for futuristic city traffic data',
    schedule_interval='@daily',
    catchup=False
) as dag:

    def _extract(**kwargs):
        df = pd.read_csv('/opt/airflow/data/futuristic_city_traffic.csv')
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())

    def _transform(**kwargs):
        raw_json = kwargs['ti'].xcom_pull(task_ids='extract', key='raw_data')
        df = pd.read_json(raw_json)
        result = df.groupby('city')['traffic_density'].mean().reset_index()
        result.columns = ['city', 'avg_traffic_density']
        kwargs['ti'].xcom_push(key='transformed_data', value=result.to_json())

    def _load(**kwargs):
        df_json = kwargs['ti'].xcom_pull(task_ids='transform', key='transformed_data')
        df = pd.read_json(df_json)
        client = bigquery.Client()
        table_id = "ace-agility-457703-q3.smart_city.traffic_data"
        job = client.load_table_from_dataframe(df, table_id)
        job.result()

        engine = create_engine('postgresql://airflow:airflow@postgres:5432/traffic_db')

        # ✅ Load into PostgreSQL
        df.to_sql(name='traffic_events', con=engine, if_exists='replace', index=False)
        print("✅ Data loaded into 'traffic_events' table.")

    extract = PythonOperator(task_id='extract', python_callable=_extract)
    transform = PythonOperator(task_id='transform', python_callable=_transform)
    load = PythonOperator(task_id='load', python_callable=_load)

    extract >> transform >> load
