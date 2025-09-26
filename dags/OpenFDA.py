from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

def fetch_openfda_food(ds, ti, **context):
    from airflow.operators.python import get_current_context
    context = get_current_context()
    execution_date = context['dag_run'].execution_date
    year = execution_date.year
    month = execution_date.month

    # intervalo mensal
    start_date = f"{year}{month:02d}01"
    end_date = f"{year}{month:02d}31"

    url = f"https://api.fda.gov/food/enforcement.json?search=report_date:[{start_date}+TO+{end_date}]&limit=100"
    r = requests.get(url)

    if r.status_code == 200:
        data = r.json()
        df = pd.DataFrame(data['results'])
        df["report_date"] = pd.to_datetime(df["report_date"])
    else:
        df = pd.DataFrame([])

    ti.xcom_push(key="openfda_food", value=df.to_dict())

def save_to_bigquery(ds, ti, **context):
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    data_dict = ti.xcom_pull(task_ids='fetch_openfda_food', key='openfda_food')
    if data_dict:
        df = pd.DataFrame.from_dict(data_dict)

        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        bq_hook.insert_rows_dataframe(
            dataset_id="openfda_dataset",
            table_id="food_enforcement",
            dataframe=df,
            project_id="mba2025-470818"
        )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'openfda_food_etl',
    default_args=default_args,
    description='ETL OpenFDA Food Enforcement para BigQuery',
    schedule_interval='@monthly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

fetch_task = PythonOperator(
    task_id="fetch_openfda_food",
    python_callable=fetch_openfda_food,
    dag=dag,
)

save_task = PythonOperator(
    task_id="save_to_bigquery",
    python_callable=save_to_bigquery,
    dag=dag,
)

fetch_task >> save_task


