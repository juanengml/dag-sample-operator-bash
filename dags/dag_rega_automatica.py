from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import logging
import requests
import time

# Timezone São Paulo (UTC-3)
SP_TZ = timezone(timedelta(hours=-3))

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Classe de controle Thingsboard
class ThingsboardControl:
    def __init__(self, device_id):
        self.base_url = "http://192.168.0.48:8080"
        self.device_id = device_id
        self.token = None
        self.headers = None

    def login(self):
        login_url = f"{self.base_url}/api/auth/login"
        credentials = {
            "username": "juanengml@gmail.com",
            "password": "tijolo22"
        }
        try:
            response = requests.post(login_url, json=credentials)
            if response.status_code == 200:
                self.token = response.json()['token']
                self.headers = {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'X-Authorization': f'Bearer {self.token}'
                }
                return True
            return False
        except Exception as e:
            logger.error(f"Erro no login: {str(e)}")
            return False

    def controlar_rele(self, method, estado):
        if not self.token and not self.login():
            raise Exception("Não foi possível autenticar")

        url = f"{self.base_url}/api/rpc/oneway/{self.device_id}"
        payload = {
            "method": method,
            "params": estado,
            "persistent": False,
            "timeout": 5000
        }

        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=10)
            if response.status_code == 200:
                logger.info(f"{method} -> {'ligado' if estado else 'desligado'} com sucesso.")
                return True
            elif response.status_code == 401:
                if self.login():
                    return self.controlar_rele(method, estado)
            raise Exception(f"Erro ao controlar relé: {response.text}")
        except Exception as e:
            raise Exception(f"Falha na requisição: {str(e)}")

# ID do dispositivo de irrigação
DEVICE_ID = "aadfcc60-c23a-11ef-b046-0502d2f0cb2b"
RELE_METHOD = "setState4"

# Funções da DAG
def ligar_rele4():
    horario = datetime.now(tz=SP_TZ).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Ligando sistema de rega às {horario}")
    controlador = ThingsboardControl(DEVICE_ID)
    return controlador.controlar_rele(RELE_METHOD, True)

def aguardar_10s():
    logger.info("Aguardando 10 segundos antes de desligar...")
    time.sleep(10)
    return True

def desligar_rele4():
    horario = datetime.now(tz=SP_TZ).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Desligando sistema de rega às {horario}")
    controlador = ThingsboardControl(DEVICE_ID)
    return controlador.controlar_rele(RELE_METHOD, False)

# DAG do Airflow
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rega_automatica",
    description="Ativa o sistema de rega automaticamente às 5h",
    default_args=default_args,
    schedule_interval="0 5 * * *",  # Todos os dias às 5h
    start_date=datetime(2025, 8, 1, 5, 0, 0, tzinfo=SP_TZ),
    catchup=False,
    tags=["rega", "jardim", "automacao"],
) as dag:

    ligar = PythonOperator(task_id="ligar_rele4", python_callable=ligar_rele4)
    esperar = PythonOperator(task_id="aguardar_10s", python_callable=aguardar_10s)
    desligar = PythonOperator(task_id="desligar_rele4", python_callable=desligar_rele4)

    ligar >> esperar >> desligar
