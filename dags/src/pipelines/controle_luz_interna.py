"""
Pipeline para controle de luz interna.
""" 
# src/pipelines/controle_luz_interna.py

import logging
from datetime import datetime
from plombery import task
from src.utils.thingsboard import ThingsboardControl

logger = logging.getLogger(__name__)

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
    horario = datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')
    logger.info(f"Controlando dispositivo {device_key} - {'Ligando' if estado else 'Desligando'} às {horario}")
    return controlador.controlar_rele(config['method'], estado)

@task
async def ligar_device1():
    return controlar_device('device1', True)

@task
async def ligar_device2():
    return controlar_device('device2', True)

@task
async def ligar_device3():
    return controlar_device('device3', True)

@task
async def desligar_device1():
    return controlar_device('device1', False)

@task
async def desligar_device2():
    return controlar_device('device2', False)

@task
async def desligar_device3():
    return controlar_device('device3', False)
