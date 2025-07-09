"""
Airflow DAG: bcb_indicadores_dag.py
----------------------------------
Extrai séries temporais do SGS (Banco Central do Brasil), agrega por mês
(equivalente à média mensal) e salva CSV consolidado.

• Indicadores capturados
  - SELIC diária (cód. 11)
  - SELIC 12 m (cód. 4390)
  - IPCA (cód. 433)
  - Dólar (cód. 1)
  - PIB (cód. 4380)
  - Poupança (cód. 195)

• Etapas da DAG (3 tasks)
  1. **extrair_dados**  → baixa e normaliza cada série
  2. **agregar_dados**  → calcula média mensal e consolida tabelas
  3. **salvar_resultado** → grava /tmp/indicadores_consolidados.csv e mostra preview

A DAG roda toda segunda‑feira às 08:00 (horário do Airflow) e utiliza
apenas `requests`, `pandas`, `datetime` e operadores nativos do Airflow.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Dict

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ──────────────────────────────────────────────────────────────────────────────
# Configurações
# ──────────────────────────────────────────────────────────────────────────────
SERIES: Dict[str, int] = {
    "selic_diaria": 11,
    "selic_12m": 4390,
    "ipca": 433,
    "dolar": 1,
    "pib": 4380,
    "poupanca": 195,
}

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DATA_INICIAL = "01/01/2020"  # formato dd/mm/aaaa
CSV_PATH = "/tmp/indicadores_consolidados.csv"

# ──────────────────────────────────────────────────────────────────────────────
# Funções das tasks
# ──────────────────────────────────────────────────────────────────────────────

def extrair_dados(**context):
    """Baixa cada série do SGS e envia DataFrames (JSON) via XCom."""

    data_final = datetime.today().strftime("%d/%m/%Y")
    dfs_raw = {}

    for nome, codigo in SERIES.items():
        url = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
            f"?formato=json&dataInicial={DATA_INICIAL}&dataFinal={data_final}"
        )
        print(f"📡 Baixando {nome} ({codigo}) …")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        df = pd.DataFrame(resp.json())
        df["data"] = pd.to_datetime(df["data"], dayfirst=True)
        df["valor"] = pd.to_numeric(df["valor"].str.replace(",", "."), errors="coerce")
        df = df.rename(columns={"valor": nome})
        dfs_raw[nome] = df[["data", nome]]

    # Serializa cada DataFrame como JSON e empurra para XCom
    context["ti"].xcom_push(
        key="series_raw",
        value={k: v.to_json() for k, v in dfs_raw.items()},
    )


def agregar_dados(**context):
    """Converte para DataFrame, agrega por mês e consolida todas as séries."""

    series_json = context["ti"].xcom_pull(key="series_raw", task_ids="extrair_dados")
    dfs_mensais = []

    for nome, json_str in series_json.items():
        df = pd.read_json(json_str)
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["ano_mes"] = df["data"].dt.to_period("M")
        df_mensal = df.groupby("ano_mes")[nome].mean().reset_index()
        dfs_mensais.append(df_mensal)

    consolidado = dfs_mensais[0]
    for df in dfs_mensais[1:]:
        consolidado = pd.merge(consolidado, df, on="ano_mes", how="outer")

    consolidado["ano_mes"] = consolidado["ano_mes"].astype(str)
    consolidado = consolidado.sort_values("ano_mes").reset_index(drop=True)

    context["ti"].xcom_push(key="consolidado", value=consolidado.to_json())


def salvar_resultado(**context):
    """Grava CSV consolidado em disco e mostra últimas linhas no log."""

    consolidado_json = context["ti"].xcom_pull(key="consolidado", task_ids="agregar_dados")
    df = pd.read_json(consolidado_json)
    df.to_csv(CSV_PATH, index=False)
    print("✅ CSV salvo em", CSV_PATH)
    print(df.tail())


# ──────────────────────────────────────────────────────────────────────────────
# Definição da DAG
# ──────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="bcb_indicadores_dag",
    description="Extrai e consolida indicadores do SGS do Banco Central",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 8 * * 1",  # segunda‑feira 08:00
    catchup=False,
    tags=["bcb", "indicadores", "etl"],
    max_active_runs=1,
) as dag:

    extrair = PythonOperator(
        task_id="extrair_dados",
        python_callable=extrair_dados,
    )

    agregar = PythonOperator(
        task_id="agregar_dados",
        python_callable=agregar_dados,
    )

    salvar = PythonOperator(
        task_id="salvar_resultado",
        python_callable=salvar_resultado,
    )

    extrair >> agregar >> salvar
