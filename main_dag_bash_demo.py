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
    dag_id='example_bash_operator_sample',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['demo', 'BashOperator'],
    params={"example_key": "example_value"},
) as dag:

    run_this_last = BashOperator(
        task_id='final',
        bash_command='echo 2',

    )

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id='meio_do_loop',
        bash_command='echo 1',
    )
    # [END howto_operator_bash]

    run_this_last >> run_this 

    for i in range(2):
        task = BashOperator(
            task_id='loop' + str(i),
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    # [START howto_operator_bash_template]
    # [END howto_operator_bash_template]
    

if __name__ == "__main__":
    dag.cli()