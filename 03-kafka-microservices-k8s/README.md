# Микросервисы потоковой обработки данных в Kubernetes

Два микросервиса для потоковой обработки заказов финтех-платформы: заполнение **DDS-** и **CDM-слоёв** DWH из Kafka. Сервисы упакованы в Docker, опубликованы в Container Registry и развёрнуты в **Yandex Managed Kubernetes**.

## 🎯 Задача

Построить потоковую обработку данных:
1. **Сервис DDS** читает обогащённые заказы из топика `stg-service-orders`, заполняет таблицы детального слоя в PostgreSQL и публикует сообщения в топик `dds-service-orders`.
2. **Сервис CDM** читает из `dds-service-orders` и заполняет витрины в PostgreSQL.

Оба сервиса **идемпотентны** — повторная доставка сообщений не создаёт дублей в DWH.

## 🏗️ Архитектура
Kafka: order-service_orders (исходные заказы)
│
▼
STG-сервис (обогащение)
│
▼
Kafka: stg-service-orders
│
▼
┌──────────────────────┐
│ Сервис DDS │──► PostgreSQL (DDS-таблицы)
│ читает из stg │
└──────────┬───────────┘
│
▼
Kafka: dds-service-orders
│
▼
┌──────────────────────┐
│ Сервис CDM │──► PostgreSQL (витрины)
│ читает из dds │
└──────────────────────┘

text

## 🛠️ Стек

- Python
- Apache Kafka (Yandex Managed Kafka)
- PostgreSQL
- Docker
- Kubernetes (Yandex Managed K8s)
- Container Registry
- Yandex DataLens (дашборд)

## 📂 Структура репозитория
.
├── service-dds/ # сервис DDS-слоя
│ ├── Dockerfile
│ ├── requirements.txt
│ └── main.py
├── service-cdm/ # сервис CDM-слоя
│ ├── Dockerfile
│ ├── requirements.txt
│ └── main.py
├── k8s/ # манифесты Kubernetes
│ ├── deployment-dds.yaml
│ └── deployment-cdm.yaml
└── README.md

text

## 🔄 Топики Kafka

| Топик | Назначение |
|-------|------------|
| `order-service_orders` | исходные заказы |
| `stg-service-orders` | обогащённые заказы |
| `dds-service-orders` | данные для DDS-слоя |

Создание топика:
```bash
yc kafka topic create dds-service-orders \
  --cluster-name kafka296 \
  --partitions 1 \
  --replication-factor 1
🚀 Как запустить
Собрать Docker-образы:

bash
docker build -t service-dds ./service-dds
docker build -t service-cdm ./service-cdm
Опубликовать в Container Registry:

bash
docker tag service-dds cr.yandex/<registry-id>/service-dds:latest
docker push cr.yandex/<registry-id>/service-dds:latest

docker tag service-cdm cr.yandex/<registry-id>/service-cdm:latest
docker push cr.yandex/<registry-id>/service-cdm:latest
Применить манифесты Kubernetes:

bash
kubectl apply -f k8s/deployment-dds.yaml
kubectl apply -f k8s/deployment-cdm.yaml
Проверить, что поды поднялись:

bash
kubectl get pods
Подключиться к PostgreSQL и проверить, что таблицы заполняются.
