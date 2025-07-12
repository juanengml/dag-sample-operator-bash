from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'srmarinho',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='job_kubernetes_duas_etapas',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["kubernetes", "k3s"],
) as dag:

    # Etapa 1
    etapa_1 = KubernetesPodOperator(
        task_id="etapa_1_echo",
        name="etapa-1-echo-pod",
        namespace="airflow",
        image="busybox",
        cmds=["sh", "-c"],
        arguments=["echo 'Etapa 1 finalizada com sucesso!'"],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Etapa 2 (só roda após a Etapa 1)
    etapa_2 = KubernetesPodOperator(
        task_id="etapa_2_sleep_echo",
        name="etapa-2-sleep-pod",
        namespace="airflow",
        image="busybox",
        cmds=["sh", "-c"],
        arguments=["sleep 5 && echo 'Etapa 2 executada depois da etapa 1!'"],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    etapa_1 >> etapa_2  # Dependência entre as tarefas
