"""
Utilitários para integração com o Thingsboard.
""" 
# src/utils/thingsboard.py

import requests

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
            print(f"Erro no login: {str(e)}")
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
            response = requests.post(url, json=payload, headers=self.headers)
            if response.status_code == 200:
                print(f"Device {self.device_id} - {method} {'ligado' if estado else 'desligado'} com sucesso!")
                return True
            elif response.status_code == 401:
                if self.login():
                    return self.controlar_rele(method, estado)
            raise Exception(f"Erro ao controlar relé: {response.text}")
        except Exception as e:
            raise Exception(f"Falha na requisição: {str(e)}")
