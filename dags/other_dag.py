from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

# Indicadores e seus códigos SGS
series = {
    "selic_diaria": 11,
    "selic_12m": 4390,
    "ipca": 433,
    "dolar": 1,
    "pib": 4380,
    "poupanca": 195
}

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def extrair_consolidar_salvar():
    data_inicial = "01/01/2020"
    data_final = datetime.today().strftime("%d/%m/%Y")

    def extrair_serie(nome, codigo):
        print(f"📡 Extraindo {nome} ({codigo})...")
        url = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
            f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
        )
        resp = requests.get(url)
        dados = resp.json()
        df = pd.DataFrame(dados)
        df["data"] = pd.to_datetime(df["data"], dayfirst=True)
        df["valor"] = pd.to_numeric(df["valor"].str.replace(",", "."), errors="coerce")
        df = df.rename(columns={"valor": nome})
        return df[["data", nome]]

    def agregar_mensal(df, nome_coluna):
        df["ano_mes"] = df["data"].dt.to_period("M")
        return df.groupby("ano_mes")[nome_coluna].mean().reset_index()

    dfs_mensais = []
    for nome, codigo in series.items():
        df_raw = extrair_serie(nome, codigo)
        df_mensal = agregar_mensal(df_raw, nome)
        dfs_mensais.append(df_mensal)

    consolidado = dfs_mensais[0]
    for df in dfs_mensais[1:]:
        consolidado = pd.merge(consolidado, df, on="ano_mes", how="outer")

    consolidado["ano_mes"] = consolidado["ano_mes"].astype(str)
    consolidado = consolidado.sort_values("ano_mes").reset_index(drop=True)

    # Salva em disco
    consolidado.to_csv("/tmp/indicadores_consolidados.csv", index=False)
    print(consolidado.tail())

with DAG(
    dag_id="bcb_indicadores_dag",
    default_args=default_args,
    schedule_interval="0 8 * * 1",  # roda toda segunda-feira às 8h
    catchup=False,
    description="Extrai e consolida indicadores do SGS do Banco Central",
    tags=["bcb", "indicadores", "etl"],
) as dag:

    tarefa_extrair_salvar = PythonOperator(
        task_id="extrair_consolidar_salvar",
        python_callable=extrair_consolidar_salvar
    )

    tarefa_extrair_salvar
