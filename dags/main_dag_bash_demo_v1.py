from datetime import timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'me',
}

with DAG(
    dag_id='elt-pipeline-iris',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['etl', 'bash', 'python'],
    params={"example_key": "example_value"},
) as dag:

    def extracao():
        print("Iniciando extração de dados...")
        df = pd.read_csv(
            "https://gist.githubusercontent.com/netj/8836201/raw/"
            "6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv"
        )
        df.to_csv("/tmp/base.csv", index=False)
        print("Extração concluída e salva em /tmp/base.csv")

    def transformacao():
        print("Iniciando transformação de dados...")
        df = pd.read_csv("/tmp/base.csv")
        df[["sepal.length", "sepal.width", "petal.length", "petal.width"]].to_csv("/tmp/base_limpa.csv", index=False)
        print("Transformação concluída e salva em /tmp/base_limpa.csv")

    extract = PythonOperator(
        task_id='extracao',
        python_callable=extracao
    )

    transform = PythonOperator(
        task_id='transformacao',
        python_callable=transformacao
    )

    load = BashOperator(
        task_id='carga',
        bash_command='cat /tmp/base_limpa.csv',
    )

    extract >> transform >> load

if __name__ == "__main__":
    dag.cli()
