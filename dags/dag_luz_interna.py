from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import logging
import requests

# Timezone de São Paulo (UTC-3)
SP_TZ = timezone(timedelta(hours=-3))

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Classe utilitária para controle via Thingsboard
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
                logger.info(f"Device {self.device_id} - {method} {'ligado' if estado else 'desligado'} com sucesso!")
                return True
            elif response.status_code == 401:
                if self.login():
                    return self.controlar_rele(method, estado)
            raise Exception(f"Erro ao controlar relé: {response.text}")
        except Exception as e:
            raise Exception(f"Falha na requisição: {str(e)}")

# Configuração dos dispositivos
DEVICES_CONFIG = {
    'device1': {
        'id': 'bda5f440-c240-11ef-b046-0502d2f0cb2b',
        'method': 'setState1',
    },
    'device2': {
        'id': 'aadfcc60-c23a-11ef-b046-0502d2f0cb2b',
        'method': 'setState3',
    },
    'device3': {
        'id': '88c78820-c307-11ef-b046-0502d2f0cb2b',
        'method': 'setState1',
    }
}

def controlar_device(device_key, estado):
    config = DEVICES_CONFIG[device_key]
    controlador = ThingsboardControl(config['id'])
    horario = datetime.now(tz=SP_TZ).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Controlando dispositivo {device_key} - {'Ligando' if estado else 'Desligando'} às {horario}")
    return controlador.controlar_rele(config['method'], estado)

# Funções para Airflow
def ligar_device1(): return controlar_device('device1', True)
def ligar_device2(): return controlar_device('device2', True)
def ligar_device3(): return controlar_device('device3', True)

def desligar_device1(): return controlar_device('device1', False)
def desligar_device2(): return controlar_device('device2', False)
def desligar_device3(): return controlar_device('device3', False)

# DAG Airflow
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="luz_interna",
    description="Liga e desliga luzes internas automaticamente",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 8, 1, 0, 0, 0, tzinfo=SP_TZ),
    catchup=False,
    tags=["automacao", "luzes"],
) as dag:

    # Tarefas de ligar
    ligar1 = PythonOperator(task_id="ligar_device1", python_callable=ligar_device1)
    ligar2 = PythonOperator(task_id="ligar_device2", python_callable=ligar_device2)
    ligar3 = PythonOperator(task_id="ligar_device3", python_callable=ligar_device3)

    # Tarefas de desligar
    desligar1 = PythonOperator(task_id="desligar_device1", python_callable=desligar_device1)
    desligar2 = PythonOperator(task_id="desligar_device2", python_callable=desligar_device2)
    desligar3 = PythonOperator(task_id="desligar_device3", python_callable=desligar_device3)

    # Encadeamento opcional
    ligar1 >> ligar2 >> ligar3
    desligar1 >> desligar2 >> desligar3
