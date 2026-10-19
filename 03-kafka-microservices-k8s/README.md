# sprint-9-project

Репозиторий для разработки проекта 9-го спринта.

### Создаем топик:

yc kafka topic create dds-service-orders \
 --cluster-name kafka296 \
 --partitions 1 \
 --replication-factor 1

### Топики Kafka:

- `order-service_orders` — исходные заказы
- `stg-service-orders` — обогащённые заказы
- `dds-service-orders` — данные для DDS

### Создать дашборд в DataLens

Очень интресная тема, ноя не успеваю пока все изучить, сделаю позже, в задании сказано, чт можно пока пропусить этот пункт
