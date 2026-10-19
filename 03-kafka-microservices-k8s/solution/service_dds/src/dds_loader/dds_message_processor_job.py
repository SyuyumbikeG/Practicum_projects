import json
from datetime import datetime
from logging import Logger
from typing import Dict, Any, Optional

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    def _parse_datetime(self, dt_str: str) -> datetime:
        """Преобразует строку в datetime."""
        if dt_str:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return datetime.utcnow()

    def _get_payload(self, msg: Dict) -> Dict:
        """Извлекает payload из сообщения."""
        return msg.get('payload', {})

    def _get_user_id(self, payload: Dict) -> Optional[str]:
        """Извлекает user_id из payload с проверкой на None."""
        user_data = payload.get('user')
        if user_data is None:
            return None
        return user_data.get('id')

    def _get_restaurant_id(self, payload: Dict) -> Optional[str]:
        """Извлекает restaurant_id из payload с проверкой на None."""
        restaurant_data = payload.get('restaurant')
        if restaurant_data is None:
            return None
        return restaurant_data.get('id')

    def _get_order_items(self, payload: Dict) -> list:
        """Извлекает order_items из payload."""
        return payload.get('order_items', [])

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        processed_count = 0

        for _ in range(self._batch_size):
            # 1. Получаем сообщение из Kafka
            msg = self._consumer.consume()
            if msg is None:
                self._logger.info("Сообщений в Kafka нет. Выхожу из цикла.")
                break

            try:
                self._logger.info(f"Обработка сообщения: {msg.get('object_id')}")

                payload = self._get_payload(msg)
                object_id = msg.get('object_id')
                sent_dttm = self._parse_datetime(msg.get('sent_dttm'))

                # ============================================
                # 2. Сохраняем хабы
                # ============================================

                # 2.1 Пользователь
                user_id = self._get_user_id(payload)
                if user_id:
                    self._dds_repository.upsert_hub_user(user_id)
                    self._logger.debug(f"Хаб пользователя: {user_id}")

                # 2.2 Ресторан
                restaurant_id = self._get_restaurant_id(payload)
                if restaurant_id:
                    self._dds_repository.upsert_hub_restaurant(restaurant_id)
                    self._logger.debug(f"Хаб ресторана: {restaurant_id}")

                # 2.3 Заказ
                order_dt = self._parse_datetime(payload.get('date'))
                self._dds_repository.upsert_hub_order(object_id, order_dt)
                self._logger.debug(f"Хаб заказа: {object_id}")

                # 2.4 Продукты и категории (исправлено)
                order_items = self._get_order_items(payload)
                for item in order_items:
                    product_id = item.get('product_id')
                    product_data = item.get('product_data')
                    category = product_data.get('category') if product_data else None

                    if product_id:
                        self._dds_repository.upsert_hub_product(product_id)
                        self._logger.debug(f"Хаб продукта: {product_id}")

                    if category:
                        self._dds_repository.upsert_hub_category(category)
                        self._logger.debug(f"Хаб категории: {category}")

                # ============================================
                # 3. Сохраняем линки
                # ============================================

                # 3.1 Заказ → Пользователь
                if user_id:
                    self._dds_repository.upsert_link_order_user(object_id, user_id)

                # 3.2 Заказ → Ресторан
                if restaurant_id:
                    self._dds_repository.upsert_link_order_restaurant(object_id, restaurant_id)

                # 3.3 Заказ → Продукт
                for item in order_items:
                    product_id = item.get('product_id')
                    if product_id:
                        self._dds_repository.upsert_link_order_product(
                            object_id,
                            product_id,
                            item.get('quantity', 0)
                        )

                # 3.4 Продукт → Категория
                for item in order_items:
                    product_id = item.get('product_id')
                    product_data = item.get('product_data')
                    category = product_data.get('category') if product_data else None
                    if product_id and category:
                        self._dds_repository.upsert_link_product_category(product_id, category)

                # ============================================
                # 4. Сохраняем сателлиты
                # ============================================

                # 4.1 Сателлит пользователя
                user_data = payload.get('user')
                if user_id and user_data:
                    user_data_dict = user_data.get('data', {})
                    if user_data_dict:
                        self._dds_repository.upsert_sat_user_names(
                            user_id,
                            user_data_dict.get('name'),
                            user_data_dict.get('login'),
                            "kafka"
                        )

                # 4.2 Сателлит ресторана
                restaurant_data = payload.get('restaurant')
                if restaurant_id and restaurant_data:
                    restaurant_data_dict = restaurant_data.get('data', {})
                    if restaurant_data_dict:
                        self._dds_repository.upsert_sat_restaurant_names(
                            restaurant_id,
                            restaurant_data_dict.get('name'),
                            "kafka"
                        )

                # 4.3 Сателлит продуктов (исправлено)
                for item in order_items:
                    product_id = item.get('product_id')
                    product_data = item.get('product_data')
                    product_name = product_data.get('name') if product_data else None
                    if product_id and product_name:
                        self._dds_repository.upsert_sat_product_names(
                            product_id,
                            product_name,
                            "kafka"
                        )

                # 4.4 Сателлит стоимости заказа
                cost = payload.get('cost', 0)
                payment = payload.get('payment', 0)
                self._dds_repository.upsert_sat_order_cost(object_id, cost, payment, "kafka")

                # 4.5 Сателлит статуса заказа
                final_status = payload.get('final_status')
                if final_status:
                    self._dds_repository.upsert_sat_order_status(object_id, final_status, "kafka")

                # ============================================
                # 5. Отправляем сообщение в Kafka (DDS -> CDM)
                # ============================================
                self._producer.produce(msg)
                self._logger.debug(f"Сообщение {object_id} отправлено в Kafka")

                processed_count += 1

            except Exception as e:
                self._logger.error(f"Ошибка обработки сообщения {msg.get('object_id')}: {e}")
                continue

        self._logger.info(f"{datetime.utcnow()}: FINISH. Обработано: {processed_count}")