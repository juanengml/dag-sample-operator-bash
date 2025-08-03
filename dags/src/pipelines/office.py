"""
Pipeline para automação do escritório.
""" 
# src/pipelines/office.py

import logging
from datetime import datetime
from plombery import task
from src.utils.thingsboard import ThingsboardControl

logger = logging.getLogger(__name__)

DEVICES_CONFIG = {
    'area_externa': {
        'device_id': 'aadfcc60-c23a-11ef-b046-0502d2f0cb2b',
        'reles': {
            'corredor': 1,
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

def controlar_dispositivo(area, rele_nome, estado):
    config = DEVICES_CONFIG[area]
    controlador = ThingsboardControl(config['device_id'])
    horario = datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')
    logger.info(f"Controlando {rele_nome} em {area} - {'Ligando' if estado else 'Desligando'} às {horario}")
    return controlador.controlar_rele(f"setState{config['reles'][rele_nome]}", estado)

@task
async def ligar_portaozinho():
    controlar_dispositivo("garagem", "luz_garagem", True)
    controlar_dispositivo("area_interna", "luz_rede", True)
    controlar_dispositivo("area_interna", "banheiro_gato", True)
    return controlar_dispositivo('area_externa', 'portaozinho', True)

@task
async def desligar_portaozinho():
    controlar_dispositivo("garagem", "luz_garagem", False)
    controlar_dispositivo("area_interna", "luz_rede", False)
    controlar_dispositivo("area_interna", "banheiro_gato", False)
    return controlar_dispositivo('area_externa', 'portaozinho', False)
