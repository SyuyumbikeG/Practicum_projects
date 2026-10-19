import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from lib.kafka_connect import KafkaConsumer
from lib.pg import PgConnect
from cdm_loader.repository.cdm_repository import CdmRepository
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor

app = Flask(__name__)


@app.get('/health')
def health():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    config = AppConfig()

    # ============================================
    # 1. Создаём зависимости
    # ============================================

    # Kafka Consumer (читаем из dds-service-orders)
    consumer = config.kafka_consumer()

    # PostgreSQL
    db = config.pg_warehouse_db()

    # Cdm Repository
    cdm_repository = CdmRepository(db)

    # ============================================
    # 2. Создаём процессор
    # ============================================

    proc = CdmMessageProcessor(
        consumer=consumer,
        cdm_repository=cdm_repository,
        batch_size=100,
        logger=app.logger
    )

    # ============================================
    # 3. Запускаем планировщик
    # ============================================

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    # ============================================
    # 4. Запускаем Flask
    # ============================================

    app.run(debug=True, host='0.0.0.0', use_reloader=False)