"""
Entrypoint do Plombery.
""" 
# src/app.py

import logging
from datetime import datetime
from plombery import register_pipeline, Trigger
from apscheduler.triggers.cron import CronTrigger
from src.pipelines import controle_luz_interna, office, rega_automatica
from plombery import get_app

# Configurar logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

# Pipeline para ligar luzes
register_pipeline(
    id="ligar_luzes_noturno",
    description="Liga as luzes internas automaticamente",
    tasks=[
        controle_luz_interna.ligar_device1,
        controle_luz_interna.ligar_device2,
        controle_luz_interna.ligar_device3
    ],
    triggers=[
        Trigger(
            id="ligar_noturno",
            name="Ligar Luzes Noturno",
            description="Liga as luzes às 17h automaticamente",
            schedule=CronTrigger(hour="17")
        )
    ]
)

# Pipeline para desligar luzes
register_pipeline(
    id="desligar_luzes_noturno",
    description="Desliga as luzes internas automaticamente",
    tasks=[
        controle_luz_interna.desligar_device1,
        controle_luz_interna.desligar_device2,
        controle_luz_interna.desligar_device3
    ],
    triggers=[
        Trigger(
            id="desligar_noturno",
            name="Desligar Luzes Noturno",
            description="Desliga as luzes às 21h automaticamente",
            schedule=CronTrigger(hour="21")
        )
    ]
)

# Pipeline para ligar iluminação externa
register_pipeline(
    id="ligar_iluminacao_externa",
    description="Liga a iluminação externa e garagem",
    tasks=[
        office.ligar_portaozinho
    ],
    triggers=[
        Trigger(
            id="ligar_iluminacao",
            name="Ligar Iluminação Externa",
            description="Liga iluminação às 17h",
            schedule=CronTrigger(hour="17")
        )
    ]
)

# Pipeline para desligar iluminação externa
register_pipeline(
    id="desligar_iluminacao_externa",
    description="Desliga a iluminação externa e garagem",
    tasks=[
        office.desligar_portaozinho
    ],
    triggers=[
        Trigger(
            id="desligar_iluminacao",
            name="Desligar Iluminação Externa",
            description="Desliga iluminação às 5h",
            schedule=CronTrigger(hour="5")
        )
    ]
)

register_pipeline(
    id="controle_rega_automatica",
    description="Controle de rega automática",
    tasks=[
        rega_automatica.ligar_rele4,
        rega_automatica.aguardar_10s,
        rega_automatica.desligar_rele4
    ],
    triggers=[
        Trigger(
            id="rega",
            name="Rega Automática",
            description="Ativa sistema de rega às 5h da manhã",
            schedule=CronTrigger(hour="5")
        )
    ]
)

if __name__ == "__main__":
    logger.info("Iniciando aplicação Plombery")
    logger.info(f"Horário atual: {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info("Pipelines registrados:")
    logger.info("- Ligar luzes noturno (17h)")
    logger.info("- Desligar luzes noturno (22h)")
    logger.info("- Ligar iluminação externa (17h)")
    logger.info("- Desligar iluminação externa (5h)")
    logger.info("- Controle rega automática (5h)")
    
    import uvicorn
    uvicorn.run("plombery:src.get_app", reload=True, factory=True)
