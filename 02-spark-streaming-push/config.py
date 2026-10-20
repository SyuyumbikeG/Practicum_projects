import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла (если он есть)
load_dotenv()

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_USER = os.getenv("KAFKA_USER", "your_kafka_user")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")  # пароль без значения по умолчанию — безопаснее
TOPIC_IN = os.getenv("TOPIC_IN", "input_topic")
TOPIC_OUT = os.getenv("TOPIC_OUT", "output_topic")

# PostgreSQL (для чтения)
POSTGRES_HOST_READ = os.getenv("POSTGRES_HOST_READ", "localhost")
POSTGRES_PORT_READ = os.getenv("POSTGRES_PORT_READ", "5432")
POSTGRES_DB_READ = os.getenv("POSTGRES_DB_READ", "de")
POSTGRES_USER_READ = os.getenv("POSTGRES_USER_READ", "your_read_user")
POSTGRES_PASSWORD_READ = os.getenv("POSTGRES_PASSWORD_READ")

# PostgreSQL (для записи)
POSTGRES_HOST_WRITE = os.getenv("POSTGRES_HOST_WRITE", "localhost")
POSTGRES_PORT_WRITE = os.getenv("POSTGRES_PORT_WRITE", "5432")
POSTGRES_DB_WRITE = os.getenv("POSTGRES_DB_WRITE", "de")
POSTGRES_USER_WRITE = os.getenv("POSTGRES_USER_WRITE", "your_write_user")
POSTGRES_PASSWORD_WRITE = os.getenv("POSTGRES_PASSWORD_WRITE")
