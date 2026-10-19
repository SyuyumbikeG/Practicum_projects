from datetime import datetime
from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def upsert_hub_user(self, user_id: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_user (h_user_pk, user_id, load_src)
                    VALUES (gen_random_uuid(), %s, %s)
                    ON CONFLICT (user_id) DO NOTHING
                    """,
                    (user_id, load_src)
                )

    def upsert_hub_restaurant(self, restaurant_id: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_src)
                    VALUES (gen_random_uuid(), %s, %s)
                    ON CONFLICT (restaurant_id) DO NOTHING
                    """,
                    (restaurant_id, load_src)
                )

    def upsert_hub_product(self, product_id: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_product (h_product_pk, product_id, load_src)
                    VALUES (gen_random_uuid(), %s, %s)
                    ON CONFLICT (product_id) DO NOTHING
                    """,
                    (product_id, load_src)
                )

    def upsert_hub_order(self, order_id: int, order_dt: datetime, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_src)
                    VALUES (gen_random_uuid(), %s, %s, %s)
                    ON CONFLICT (order_id) DO UPDATE SET
                        order_dt = EXCLUDED.order_dt
                    """,
                    (order_id, order_dt, load_src)
                )

    def upsert_hub_category(self, category_name: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_category (h_category_pk, category_name, load_src)
                    VALUES (gen_random_uuid(), %s, %s)
                    ON CONFLICT (category_name) DO NOTHING
                    """,
                    (category_name, load_src)
                )

    def upsert_link_order_user(self, order_id: int, user_id: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_user (h_order_pk, h_user_pk, load_src)
                    SELECT h_order.h_order_pk, h_user.h_user_pk, %s
                    FROM dds.h_order, dds.h_user
                    WHERE h_order.order_id = %s AND h_user.user_id = %s
                    ON CONFLICT (h_order_pk, h_user_pk) DO NOTHING
                    """,
                    (load_src, order_id, user_id)
                )

    def upsert_link_order_restaurant(self, order_id: int, restaurant_id: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_restaurant (h_order_pk, h_restaurant_pk, load_src)
                    SELECT h_order.h_order_pk, h_restaurant.h_restaurant_pk, %s
                    FROM dds.h_order, dds.h_restaurant
                    WHERE h_order.order_id = %s AND h_restaurant.restaurant_id = %s
                    ON CONFLICT (h_order_pk, h_restaurant_pk) DO NOTHING
                    """,
                    (load_src, order_id, restaurant_id)
                )

    def upsert_link_order_product(self, order_id: int, product_id: str, quantity: int, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_product (h_order_pk, h_product_pk, quantity, load_src)
                    SELECT h_order.h_order_pk, h_product.h_product_pk, %s, %s
                    FROM dds.h_order, dds.h_product
                    WHERE h_order.order_id = %s AND h_product.product_id = %s
                    ON CONFLICT (h_order_pk, h_product_pk) DO UPDATE SET
                        quantity = EXCLUDED.quantity
                    """,
                    (quantity, load_src, order_id, product_id)
                )

    def upsert_link_product_category(self, product_id: str, category_name: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_category (h_product_pk, h_category_pk, load_src)
                    SELECT h_product.h_product_pk, h_category.h_category_pk, %s
                    FROM dds.h_product, dds.h_category
                    WHERE h_product.product_id = %s AND h_category.category_name = %s
                    ON CONFLICT (h_product_pk, h_category_pk) DO NOTHING
                    """,
                    (load_src, product_id, category_name)
                )

    def upsert_sat_user_names(self, user_id: str, username: str, userlogin: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_src)
                    SELECT h_user_pk, %s, %s, %s
                    FROM dds.h_user
                    WHERE user_id = %s
                    ON CONFLICT (h_user_pk, load_dt) DO NOTHING
                    """,
                    (username, userlogin, load_src, user_id)
                )

    def upsert_sat_restaurant_names(self, restaurant_id: str, name: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_src)
                    SELECT h_restaurant_pk, %s, %s
                    FROM dds.h_restaurant
                    WHERE restaurant_id = %s
                    ON CONFLICT (h_restaurant_pk, load_dt) DO NOTHING
                    """,
                    (name, load_src, restaurant_id)
                )

    def upsert_sat_product_names(self, product_id: str, name: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_product_names (h_product_pk, name, load_src)
                    SELECT h_product_pk, %s, %s
                    FROM dds.h_product
                    WHERE product_id = %s
                    ON CONFLICT (h_product_pk, load_dt) DO NOTHING
                    """,
                    (name, load_src, product_id)
                )

    def upsert_sat_order_cost(self, order_id: int, cost: float, payment: float, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_src)
                    SELECT h_order_pk, %s, %s, %s
                    FROM dds.h_order
                    WHERE order_id = %s
                    ON CONFLICT (h_order_pk, load_dt) DO NOTHING
                    """,
                    (cost, payment, load_src, order_id)
                )

    def upsert_sat_order_status(self, order_id: int, status: str, load_src: str = "kafka") -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_status (h_order_pk, status, load_src)
                    SELECT h_order_pk, %s, %s
                    FROM dds.h_order
                    WHERE order_id = %s
                    ON CONFLICT (h_order_pk, load_dt) DO NOTHING
                    """,
                    (status, load_src, order_id)
                )