"""
Airflow DAG: random_user_pipeline_dag.py
---------------------------------------
Pipeline leve de 6 etapas que consulta a API https://randomuser.me/api/0.8/?results=10
utilizando apenas bibliotecas nativas (`datetime`, `json`, `requests`) e baixo uso de memória.

Adaptado para ambientes com até ~252MB de RAM.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

API_URL = "https://randomuser.me/api/0.8/?results=5"
LOCAL_PATH = "/tmp/random_users.json"


def fetch_users(**context):
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    context["ti"].xcom_push(key="raw_json", value=response.text)


def parse_users(**context):
    raw = context["ti"].xcom_pull(key="raw_json", task_ids="fetch_users")
    users = json.loads(raw).get("results", [])
    # já reduz para campos desejados aqui para economizar RAM
    slim = [
        {
            "first_name": u["user"]["name"]["first"],
            "last_name": u["user"]["name"]["last"],
            "email": u["user"]["email"],
            "username": u["user"]["username"],
        }
        for u in users
    ]
    context["ti"].xcom_push(key="slim_users", value=slim)


def save_to_local(**context):
    slim = context["ti"].xcom_pull(key="slim_users", task_ids="parse_users")
    with open(LOCAL_PATH, "w", encoding="utf-8") as fp:
        json.dump(slim, fp, separators=(",", ":"), ensure_ascii=False)
    logging.info("Arquivo salvo em %s", LOCAL_PATH)


def validate_count(**context):
    slim = context["ti"].xcom_pull(key="slim_users", task_ids="parse_users")
    if len(slim) != 5:
        raise ValueError(f"Esperado 5 usuários, recebido {len(slim)}")
    logging.info("Validação OK: 5 usuários recebidos.")


def print_summary(**context):
    slim = context["ti"].xcom_pull(key="slim_users", task_ids="parse_users")
    exemplo = json.dumps(slim[0], indent=None, ensure_ascii=False)
    logging.info("%d usuários processados. Exemplo: %s", len(slim), exemplo)


with DAG(
    dag_id="random_user_pipeline",
    description="Pipeline otimizado para coletar e salvar usuários aleatórios.",
    start_date=days_ago(1),
    schedule_interval="0 8 * * 1",
    catchup=False,
    tags=["randomuser", "leve"],
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(seconds=10),
    },
    max_active_runs=1,
    concurrency=1,
) as dag:

    fetch = PythonOperator(
        task_id="fetch_users",
        python_callable=fetch_users,
    )

    parse = PythonOperator(
        task_id="parse_users",
        python_callable=parse_users,
    )

    save = PythonOperator(
        task_id="save_to_local",
        python_callable=save_to_local,
    )

    validate = PythonOperator(
        task_id="validate_count",
        python_callable=validate_count,
    )

    resumo = PythonOperator(
        task_id="print_summary",
        python_callable=print_summary,
    )

    fetch >> parse >> save >> validate >> resumo
