## lista de demos
## https://gist.github.com/sakethramanujam/7c21497dbd11d9b7d93b57c437d2c4dd

from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Debora',
}

with DAG(
    dag_id='example_bash_etl_deb',
    default_args=args,
    schedule_interval='@hourly',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['etl', 'BashOperator'],
    params={"example_key": "example_value"},
) as dag:

    def extracao():
       print("Hello Airflow Using a Python Operator!\nextracao")
         
    def transformacao():
       print("Hello Airflow Using a Python Operator!\ntransformacao")

    def load():
       print("Hello Airflow Using a Python Operator!\nLOAD")


    extract = PythonOperator(task_id='extract',
                             python_callable=extracao)
    trans = PythonOperator(task_id='task-trans',
                             python_callable=transformacao)

    load = BashOperator(
        task_id='carga',
        bash_command='echo carga',
    )
      
    # test   

    extract >> trans >> load

if __name__ == "__main__":
    dag.cli()