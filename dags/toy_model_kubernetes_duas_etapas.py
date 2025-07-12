from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'srmarinho',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='toy_model_kubernetes_duas_etapas',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["kubernetes", "k3s", "ml"],
) as dag:

    # Etapa 1: Treinamento com modo verboso
    etapa_1_verbose = KubernetesPodOperator(
        task_id="etapa_1_verbose",
        name="etapa-1-verbose-pod",
        namespace="airflow",
        image="srmarinho/toy-model:1.0.0",
        cmds=[],
        arguments=[
            "--verbose",
            "--noise-level", "0.02",
            "--n-estimators", "100",
            "--output-dir", "/tmp/model_verbose"
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        labels={"app": "toy-model"},
    )

    # Etapa 2: Treinamento com noise level 0.05
    etapa_2_noise = KubernetesPodOperator(
        task_id="etapa_2_noise",
        name="etapa-2-noise-pod",
        namespace="airflow",
        image="srmarinho/toy-model:1.0.0",
        cmds=[],
        arguments=[
            "--noise-level", "0.05",
            "--n-estimators", "150",
            "--output-dir", "/tmp/model_noise"
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        labels={"app": "toy-model"},
    )

    # Etapa 3: Treinamento com ambos (verboso + noise alto)
    etapa_3_ambos = KubernetesPodOperator(
        task_id="etapa_3_ambos",
        name="etapa-3-ambos-pod",
        namespace="airflow",
        image="srmarinho/toy-model:1.0.0",
        cmds=[],
        arguments=[
            "--verbose",
            "--noise-level", "0.05",
            "--n-estimators", "200",
            "--output-dir", "/tmp/model_ambos"
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        labels={"app": "toy-model"},
    )

    # Dependências: Etapa 1 e 2 rodam em paralelo, Etapa 3 só depois das duas
    etapa_1_verbose >> etapa_3_ambos
    etapa_2_noise >> etapa_3_ambos 
