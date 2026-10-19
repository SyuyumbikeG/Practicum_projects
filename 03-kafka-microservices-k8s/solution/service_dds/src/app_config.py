import os

from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect


class AppConfig:
    CERTIFICATE_PATH = '/crt/YandexInternalRootCA.crt'
    DEFAULT_JOB_INTERVAL = 25

    def __init__(self) -> None:
        # Kafka
        self.kafka_host = str(os.getenv('KAFKA_HOST') or "")
        self.kafka_port = int(str(os.getenv('KAFKA_PORT')) or 0)
        self.kafka_consumer_username = str(os.getenv('KAFKA_CONSUMER_USERNAME') or "")
        self.kafka_consumer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD') or "")
        
        # ВРЕМЕННО — захардкожены для отладки!
        self.kafka_consumer_topic = "stg-service-orders"
        self.kafka_producer_topic = "dds-service-orders"
        self.kafka_consumer_group = "dds-consumer-group"
        
        self.kafka_producer_username = str(os.getenv('KAFKA_CONSUMER_USERNAME') or "")
        self.kafka_producer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD') or "")

        # PostgreSQL
        self.pg_warehouse_host = str(os.getenv('PG_WAREHOUSE_HOST') or "")
        self.pg_warehouse_port = int(str(os.getenv('PG_WAREHOUSE_PORT')) or 0)
        self.pg_warehouse_dbname = str(os.getenv('PG_WAREHOUSE_DBNAME') or "")
        self.pg_warehouse_user = str(os.getenv('PG_WAREHOUSE_USER') or "")
        self.pg_warehouse_password = str(os.getenv('PG_WAREHOUSE_PASSWORD') or "")

    def kafka_producer(self):
        return KafkaProducer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_producer_username,
            self.kafka_producer_password,
            self.kafka_producer_topic,
            self.CERTIFICATE_PATH
        )

    def kafka_consumer(self):
        return KafkaConsumer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_consumer_username,
            self.kafka_consumer_password,
            self.kafka_consumer_topic,
            self.kafka_consumer_group,
            self.CERTIFICATE_PATH
        )

    def pg_warehouse_db(self):
        return PgConnect(
            self.pg_warehouse_host,
            self.pg_warehouse_port,
            self.pg_warehouse_dbname,
            self.pg_warehouse_user,
            self.pg_warehouse_password
        )