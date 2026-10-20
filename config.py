import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Kafka
KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
)
KAFKA_USER = os.getenv("KAFKA_USER", "de-student")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "ltcneltyn")

TOPIC_IN = os.getenv("TOPIC_IN", "s18151825_in")
TOPIC_OUT = os.getenv("TOPIC_OUT", "s18151825_out")

# PostgreSQL (облачный для чтения)
POSTGRES_HOST_READ = os.getenv(
    "POSTGRES_HOST_READ", "rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net"
)
POSTGRES_PORT_READ = os.getenv("POSTGRES_PORT_READ", "6432")
POSTGRES_DB_READ = os.getenv("POSTGRES_DB_READ", "de")
POSTGRES_USER_READ = os.getenv("POSTGRES_USER_READ", "student")
POSTGRES_PASSWORD_READ = os.getenv("POSTGRES_PASSWORD_READ", "de-student")

# PostgreSQL (локальный для записи)
POSTGRES_HOST_WRITE = os.getenv("POSTGRES_HOST_WRITE", "localhost")
POSTGRES_PORT_WRITE = os.getenv("POSTGRES_PORT_WRITE", "5432")
POSTGRES_DB_WRITE = os.getenv("POSTGRES_DB_WRITE", "de")
POSTGRES_USER_WRITE = os.getenv("POSTGRES_USER_WRITE", "jovyan")
POSTGRES_PASSWORD_WRITE = os.getenv("POSTGRES_PASSWORD_WRITE", "jovyan")
