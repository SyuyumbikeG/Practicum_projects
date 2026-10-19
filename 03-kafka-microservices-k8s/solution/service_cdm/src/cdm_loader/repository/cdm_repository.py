from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def upsert_user_product_counter(self,
                                    user_id: str,
                                    product_id: str,
                                    product_name: str,
                                    order_cnt: int
                                    ) -> None:
        """Обновляет счётчик заказов по продуктам для пользователя."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.user_product_counters 
                        (user_id, product_id, product_name, order_cnt)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, product_id) DO UPDATE SET
                        order_cnt = cdm.user_product_counters.order_cnt + EXCLUDED.order_cnt,
                        product_name = EXCLUDED.product_name
                    """,
                    (user_id, product_id, product_name, order_cnt)
                )

    def upsert_user_category_counter(self,
                                     user_id: str,
                                     category_name: str,
                                     order_cnt: int
                                     ) -> None:
        """Обновляет счётчик заказов по категориям для пользователя."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.user_category_counters 
                        (user_id, category_name, order_cnt)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, category_name) DO UPDATE SET
                        order_cnt = cdm.user_category_counters.order_cnt + EXCLUDED.order_cnt
                    """,
                    (user_id, category_name, order_cnt)
                )