"""
OpenFDA Food Enforcement DAG
Adaptada para Airflow 2.7+
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import pandas as pd
import requests


# -------------------------------
# Função para coletar dados da API
# -------------------------------
def fetch_openfda_food(**kwargs):
    context = get_current_context()
    execution_date = context["logical_date"]
    year = execution_date.year
    month = execution_date.month

    # intervalo mensal
    start_date = f"{year}{month:02d}01"
    end_date = f"{year}{month:02d}31"

    url = f"https://api.fda.gov/food/enforcement.json?search=report_date:[{start_date}+TO+{end_date}]&limit=100"
    r = requests.get(url)

    if r.status_code == 200:
        data = r.json()
        df = pd.DataFrame(data.get("results", []))
        if not df.empty:
            # garante que report_date é string serializável
            df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").astype(str)
    else:
        df = pd.DataFrame([])

    # envia para o XCom em formato seguro
    kwargs["ti"].xcom_push(key="openfda_food", value=df.to_dict(orient="list"))


# -------------------------------
# Função para salvar no BigQuery
# -------------------------------
def save_to_bigquery(**kwargs):
    data_dict = kwargs["ti"].xcom_pull(task_ids="fetch_openfda_food", key="openfda_food")

    if data_dict:
        df = pd.DataFrame.from_dict(data_dict)
        if not df.empty:
            # reconverte a coluna para datetime
            df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")

            bq_hook = BigQueryHook(
                gcp_conn_id="google_cloud_default",
                use_legacy_sql=False
            )

            bq_hook.insert_rows_dataframe(
                dataset_id="openfda_dataset",
                table_id="food_enforcement",
                dataframe=df,
                project_id="SEU_PROJETO_ID"  # <-- ajuste aqui
            )


# -------------------------------
# Configuração da DAG
# -------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="openfda_food_etl",
    default_args=default_args,
    description="ETL OpenFDA Food Enforcement para BigQuery",
    schedule="@monthly",               # substitui schedule_interval
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["openfda", "food", "bigquery"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_openfda_food",
        python_callable=fetch_openfda_food,
    )

    save_task = PythonOperator(
        task_id="save_to_bigquery",
        python_callable=save_to_bigquery,
    )

    fetch_task >> save_task





