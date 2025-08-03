"""
Pipeline para sistema de rega automática.
""" 
# src/pipelines/rega_automatica.py

import time
import logging
from datetime import datetime
from plombery import task
from src.utils.thingsboard import ThingsboardControl

logger = logging.getLogger(__name__)

@task
async def ligar_rele4():
    horario = datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')
    logger.info(f"Iniciando sistema de rega automática às {horario}")
    controlador = ThingsboardControl("aadfcc60-c23a-11ef-b046-0502d2f0cb2b")
    return controlador.controlar_rele("setState4", True)

@task
async def aguardar_10s():
    logger.info("Aguardando 10 segundos...")
    time.sleep(10)
    return True

@task
async def desligar_rele4():
    horario = datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')
    logger.info(f"Finalizando sistema de rega automática às {horario}")
    controlador = ThingsboardControl("aadfcc60-c23a-11ef-b046-0502d2f0cb2b")
    return controlador.controlar_rele("setState4", False)
