"""Setup helpers shared by the course notebooks.

Kept out of the notebooks so each lesson can focus on Iceberg itself.
"""
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

CATALOG = "demo"
NAMESPACE = "course"

ORDERS_DDL = (
    "order_id long, customer string, product string, "
    "quantity int, unit_price double, order_ts timestamp"
)

_CUSTOMERS = ["alice", "bob", "carol", "dave"]
_PRODUCTS = {"keyboard": 49.0, "mouse": 19.5, "monitor": 189.0, "laptop": 999.0}


def get_spark(app_name="iceberg-course"):
    """Return a Spark session and make sure the course namespace exists."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    return spark


def full_name(table):
    return f"{CATALOG}.{NAMESPACE}.{table}"


def sample_orders(n=6, start_id=1001, seed=7):
    """Deterministic fake orders: (order_id, customer, product, quantity, unit_price, order_ts)."""
    rng = random.Random(seed)
    start = datetime(2024, 1, 1, 9, 0, 0)
    rows = []
    for i in range(n):
        product = rng.choice(sorted(_PRODUCTS))
        rows.append(
            (
                start_id + i,
                rng.choice(_CUSTOMERS),
                product,
                rng.randint(1, 4),
                _PRODUCTS[product],
                start + timedelta(hours=7 * i),
            )
        )
    return rows


def reset_orders(spark, table="orders", n=6, commits=1):
    """Drop and recreate an Iceberg orders table, loading it in `commits` separate appends.

    Every append is its own Iceberg snapshot, which the time travel lessons rely on.
    Returns the fully qualified table name.
    """
    name = full_name(table)
    spark.sql(f"DROP TABLE IF EXISTS {name} PURGE")
    spark.sql(f"CREATE TABLE {name} ({ORDERS_DDL}) USING iceberg")
    rows = sample_orders(n)
    size = -(-len(rows) // commits)
    for i in range(0, len(rows), size):
        spark.createDataFrame(rows[i : i + size], schema=ORDERS_DDL).coalesce(1).writeTo(name).append()
    return name


def drop_table(spark, table):
    spark.sql(f"DROP TABLE IF EXISTS {full_name(table)} PURGE")
