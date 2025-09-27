# dags/food_recalls_openfda_to_gbq.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import timedelta
import pendulum
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# ====== CONFIG ======
GCP_PROJECT  = "mba2025-470818"         # seu projeto GCP
BQ_DATASET   = "food"                   # dataset no BigQuery
BQ_TABLE     = "food_recalls"           # tabela destino
BQ_LOCATION  = "US"                     # dataset location
GCP_CONN_ID  = "google_cloud_default"   # conexão no Airflow
# ====================

DEFAULT_ARGS = {
    "email_on_failure": True,
    "owner": "Alex Lopes, Open in Cloud IDE",
}

@task
def fetch_and_to_gbq():
    """
    Busca recalls de alimentos no OpenFDA e carrega no BigQuery
    """
    ctx = get_current_context()
    end_time = ctx["data_interval_start"]
    start_time = end_time - timedelta(days=1)
    print(f"[UTC] target window: {start_time} -> {end_time}")

    # API OpenFDA (Food Enforcement Reports / Recalls)
    url = "https://api.fda.gov/food/enforcement.json"
    params = {"limit": 100}  # limite de registros por chamada

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    results = payload.get("results", [])
    if not results:
        print("Nenhum dado retornado pela API.")
        return

    # Converter para DataFrame
    df = pd.json_normalize(results)

    # Selecionar colunas de interesse
    colunas_interesse = [
        "recall_number",
        "status",
        "classification",
        "recalling_firm",
        "product_description",
        "reason_for_recall",
        "distribution_pattern",
        "city",
        "state",
        "country",
        "recall_initiation_date",
        "report_date",
    ]
    df = df[[c for c in colunas_interesse if c in df.columns]]

    # Ajustar tipos de datas
    for col in ["recall_initiation_date", "report_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", format="%Y%m%d", utc=True)

    # Preview no log
    print(df.head(10).to_string())

    # Conectar ao BigQuery
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()
    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"

    # Esquema da tabela
    table_schema = [
        {"name": "recall_number",        "type": "STRING"},
        {"name": "status",               "type": "STRING"},
        {"name": "classification",       "type": "STRING"},
        {"name": "recalling_firm",       "type": "STRING"},
        {"name": "product_description",  "type": "STRING"},
        {"name": "reason_for_recall",    "type": "STRING"},
        {"name": "distribution_pattern", "type": "STRING"},
        {"name": "city",                 "type": "STRING"},
        {"name": "state",                "type": "STRING"},
        {"name": "country",              "type": "STRING"},
        {"name": "recall_initiation_date","type": "TIMESTAMP"},
        {"name": "report_date",          "type": "TIMESTAMP"},
    ]

    # Salvar no BigQuery
    df.to_gbq(
        destination_table=destination_table,
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=credentials,
        table_schema=table_schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )

    print(f"Loaded {len(df)} rows to {GCP_PROJECT}.{destination_table} (location={BQ_LOCATION}).")


@dag(
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",  # roda diariamente à meia-noite UTC
    start_date=pendulum.datetime(2025, 9, 17, tz="UTC"),
    catchup=True,
    owner_links={
        "Alex Lopes": "mailto:alexlopespereira@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io",
    },
    tags=["food", "recalls", "etl", "openfda", "bigquery"],
)
def food_recalls_etl_bigquery():
    fetch_and_to_gbq()

dag = food_recalls_etl_bigquery()





