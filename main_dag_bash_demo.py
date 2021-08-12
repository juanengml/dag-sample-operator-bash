## lista de demos
## https://gist.github.com/sakethramanujam/7c21497dbd11d9b7d93b57c437d2c4dd

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='example_bash_etlc',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['etl', 'BashOperator'],
    params={"example_key": "example_value"},
) as dag:

    extracao_task = BashOperator(
        task_id='extracao',
        bash_command='wget https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv',
    )

    transformacao = BashOperator(
        task_id='transformacao',
        bash_command='cat iris.csv ',
    )

    load = BashOperator(
        task_id='carga',
        bash_command='echo carga',
    )
      

    extracao_task >> transformacao >> load

if __name__ == "__main__":
    dag.cli()