from datetime import datetime
from typing import Dict, List

from lib import MongoConnect


class UserReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_data(self, load_threshold: datetime, limit: int) -> List[Dict]:
        # Формируем фильтр: больше чем дата последней загрузки
        filter = {"update_ts": {"$gt": load_threshold}}

        # Формируем сортировку по update_ts. Сортировка обязательна при инкрементальной загрузке.
        sort = [("update_ts", 1)]

        # Вычитываем документы из MongoDB с применением фильтра и сортировки.
        docs = list(
            self.dbs.get_collection("users").find(filter=filter, sort=sort, limit=limit)
        )

        # Преобразуем в нужный формат
        result = []
        for doc in docs:
            result.append(
                {
                    "object_id": str(doc["_id"]),
                    "object_value": doc,
                    "update_ts": doc.get("update_ts", datetime.now()),
                }
            )
        return result
