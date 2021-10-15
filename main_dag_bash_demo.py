## lista de demos
## https://gist.github.com/sakethramanujam/7c21497dbd11d9b7d93b57c437d2c4dd

from datetime import timedelta
import pandas as pd 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.papermill_operator import PapermillOperator


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
       df = pd.read_csv("https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv")
       df.to_csv("base.csv")  

    def transformacao():
       print("Hello Airflow Using a Python Operator!\ntransformacao")
       df = pd.read_csv("base.csv")
       df[["sepal.length","sepal.width","petal.length","petal.width"]].to_csv("base_limpa.csv")

    def load():
       print("Hello Airflow Using a Python Operator!\nLOAD")

    notebook =  PapermillOperator(
        task_id="run_example_notebook",
        input_nb="hello_world.ipynb",
        output_nb="out-{{ execution_date }}.ipynb",
        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"}
    )

    extract = PythonOperator(task_id='extract',
                             python_callable=extracao)
    trans = PythonOperator(task_id='task-trans',
                             python_callable=transformacao)
 
    load = BashOperator(
        task_id='carga',
        bash_command='cat base_limpa.csv',
    )
      
    # test   

    extract >> trans >> load >> notebook

if __name__ == "__main__":
    dag.cli()