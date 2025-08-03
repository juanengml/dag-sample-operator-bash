from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import logging
import requests

# Fuso horário de São Paulo (UTC-3)
SP_TZ = timezone(timedelta(hours=-3))

# Logging
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

# Dispositivos e relés
DEVICES_CONFIG = {
    'area_externa': {
        'device_id': 'aadfcc60-c23a-11ef-b046-0502d2f0cb2b',
        'reles': {
            'portaozinho': 2
        }
    },
    'garagem': {
        'device_id': 'bda5f440-c240-11ef-b046-0502d2f0cb2b',
        'reles': {
            'luz_garagem': 2
        }
    },
    'area_interna': {
        'device_id': 'f71c0fd0-c30c-11ef-b046-0502d2f0cb2b',
        'reles': {
            'luz_rede': 1,
            'banheiro_gato': 2
        }
    }
}

def controlar(area, rele_nome, estado):
    config = DEVICES_CONFIG[area]
    controlador = ThingsboardControl(config['device_id'])
    metodo = f"setState{config['reles'][rele_nome]}"
    horario = datetime.now(tz=SP_TZ).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"{'Ligando' if estado else 'Desligando'} {rele_nome} de {area} às {horario}")
    return controlador.controlar_rele(metodo, estado)

# Funções para Airflow
def ligar_iluminacao():
    controlar("garagem", "luz_garagem", True)
    controlar("area_interna", "luz_rede", True)
    controlar("area_interna", "banheiro_gato", True)
    return controlar("area_externa", "portaozinho", True)

def desligar_iluminacao():
    controlar("garagem", "luz_garagem", False)
    controlar("area_interna", "luz_rede", False)
    controlar("area_interna", "banheiro_gato", False)
    return controlar("area_externa", "portaozinho", False)

# DAG do Airflow
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="iluminacao_externa",
    description="Liga e desliga iluminação externa e interna relacionada",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 8, 1, 0, 0, 0, tzinfo=SP_TZ),
    catchup=False,
    tags=["automacao", "iluminacao"],
) as dag:

    ligar = PythonOperator(task_id="ligar_iluminacao", python_callable=ligar_iluminacao)
    desligar = PythonOperator(task_id="desligar_iluminacao", python_callable=desligar_iluminacao)

    ligar >> desligar
