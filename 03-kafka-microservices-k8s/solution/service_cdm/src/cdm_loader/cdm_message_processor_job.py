import json
from datetime import datetime
from logging import Logger
from typing import Dict, Optional

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        
        # ОТЛАДКА: проверяем консьюмер
        self._logger.info(f"Консьюмер: {self._consumer}")
        self._logger.info(f"Топик консьюмера: {self._consumer.topic if hasattr(self._consumer, 'topic') else 'неизвестен'}")

        processed_count = 0

        for _ in range(self._batch_size):
            # ОТЛАДКА: пытаемся получить сообщение
            self._logger.info("Пытаюсь получить сообщение из Kafka...")
            msg = self._consumer.consume()
            self._logger.info(f"Получено сообщение: {msg}")
            
            if msg is None:
                self._logger.info("Сообщений в Kafka нет. Выхожу из цикла.")
                break

            try:
                self._logger.info(f"Обработка сообщения: {msg.get('object_id')}")

                payload = msg.get('payload', {})
                user_id = payload.get('user', {}).get('id')
                if not user_id:
                    self._logger.warning("Сообщение без user_id, пропускаем")
                    continue

                order_items = payload.get('order_items', [])

                # Собираем счётчики по продуктам и категориям для этого заказа
                product_counts = {}
                category_counts = {}

                for item in order_items:
                    product_id = item.get('product_id')
                    quantity = item.get('quantity', 0)
                    product_data = item.get('product_data', {})
                    product_name = product_data.get('name') if product_data else None
                    category = product_data.get('category') if product_data else None

                    if product_id:
                        product_counts[product_id] = {
                            'count': product_counts.get(product_id, {}).get('count', 0) + quantity,
                            'name': product_name
                        }

                    if category:
                        category_counts[category] = category_counts.get(category, 0) + quantity

                # Обновляем витрину по продуктам
                for product_id, data in product_counts.items():
                    product_name = data['name'] or f"Product_{product_id}"
                    self._cdm_repository.upsert_user_product_counter(
                        user_id,
                        product_id,
                        product_name,
                        data['count']
                    )

                # Обновляем витрину по категориям
                for category, count in category_counts.items():
                    self._cdm_repository.upsert_user_category_counter(
                        user_id,
                        category,
                        count
                    )

                processed_count += 1

            except Exception as e:
                self._logger.error(f"Ошибка обработки сообщения {msg.get('object_id')}: {e}")
                continue

        self._logger.info(f"{datetime.utcnow()}: FINISH. Обработано: {processed_count}")
