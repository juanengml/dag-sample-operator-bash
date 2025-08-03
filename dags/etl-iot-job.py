from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'deb',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='etl_tb_to_mariadb_job_kubernetes_10min',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/10 * * * *",  # a cada 10 minutos
    catchup=False,
    tags=["iot","sensores","thingsboard", "etl", "kubernetes","tb"],
) as dag:

    etl_iot_job = KubernetesPodOperator(
        task_id="run_etl_iot_job",
        name="etl-iot-pod",
        namespace="airflow",
        image="debora9rodrigues/etl-iot-job:latest",
        cmds=[],
        arguments=[
            "--username", "juanengml@gmail.com",
            "--password", "tijolo22",
            "--tb-url", "http://192.168.0.48:8080",
            "--db-user", "root",
            "--db-password", "casaos",
            "--db-host", "192.168.0.48",
            "--db-port", "3307",
            "--db-name", "Iot",
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        labels={"app": "etl-iot-job"},
    )
