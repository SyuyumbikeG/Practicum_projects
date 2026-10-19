from datetime import datetime
from typing import Dict, List

from lib import MongoConnect


class OrderReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_data(self, load_threshold: datetime, limit: int) -> List[Dict]:
        filter = {"update_ts": {"$gt": load_threshold}}
        sort = [("update_ts", 1)]
        docs = list(self.dbs.get_collection("orders").find(filter=filter, sort=sort, limit=limit))

        result = []
        for doc in docs:
            result.append({
                "object_id": str(doc["_id"]),
                "object_value": doc,
                "update_ts": doc.get("update_ts", datetime.now()),
            })
        return result