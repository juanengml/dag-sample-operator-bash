
from __future__ import annotations

import json
import logging
from datetime import timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# === Configurações gerais ===
API_URL = "https://randomuser.me/api/0.8/?results=10"
LOCAL_PATH = "/tmp/random_users.json"  # Altere para bucket S3 ou diretório de sua preferência


def fetch_users(**context):
    """Faz requisição GET na API e envia o JSON cru para XCom."""
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    context["ti"].xcom_push(key="raw_json", value=response.text)


def parse_users(**context):
    """Converte o JSON cru em Python dict e extrai a lista de usuários."""
    raw = context["ti"].xcom_pull(key="raw_json", task_ids="fetch_users")
    data = json.loads(raw)
    users = data["results"]
    context["ti"].xcom_push(key="users", value=users)


def select_fields(**context):
    """Mantém apenas campos relevantes para análise ou downstream loading."""
    users = context["ti"].xcom_pull(key="users", task_ids="parse_users")
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
    """Salva JSON enxuto em disco. Pode ser adaptado para S3, GCS, DB, etc."""
    slim = context["ti"].xcom_pull(key="slim_users", task_ids="select_fields")
    with open(LOCAL_PATH, "w", encoding="utf-8") as fp:
        json.dump(slim, fp, indent=2, ensure_ascii=False)
    logging.info("Arquivo salvo em %s", LOCAL_PATH)


def validate_count(**context):
    """Valida se recebemos exatamente 10 usuários da API."""
    slim = context["ti"].xcom_pull(key="slim_users", task_ids="select_fields")
    assert len(slim) == 10, f"Esperado 10 usuários, recebido {len(slim)}"
    logging.info("Validação OK: 10 usuários recebidos.")


def print_summary(**context):
    """Loga um resumo simples: quantidade e primeiro registro como exemplo."""
    slim = context["ti"].xcom_pull(key="slim_users", task_ids="select_fields")
    logging.info("%d usuários processados. Exemplo de registro:%s", len(slim), json.dumps(slim[0], indent=2, ensure_ascii=False))

with DAG(
    dag_id="random_user_pipeline",
    description="Pipeline de 6 etapas para coletar e processar usuários aleatórios.",
    start_date=days_ago(1),  # start backfill em D‑1
    schedule_interval=None,  # Executa on‑demand
    catchup=False,
    tags=["example", "randomuser"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1,
    concurrency=1,
) as dag:

    t1 = PythonOperator(
        task_id="fetch_users",
        python_callable=fetch_users,
    )

    t2 = PythonOperator(
        task_id="parse_users",
        python_callable=parse_users,
    )

    t3 = PythonOperator(
        task_id="select_fields",
        python_callable=select_fields,
    )

    t4 = PythonOperator(
        task_id="save_to_local",
        python_callable=save_to_local,
    )

    t5 = PythonOperator(
        task_id="validate_count",
        python_callable=validate_count,
    )

    t6 = PythonOperator(
        task_id="print_summary",
        python_callable=print_summary,
    )

    # Orquestração
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
