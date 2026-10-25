import dlt
from pyspark.sql import DataFrame
CATALOG = spark.conf.get("catalog_name", "dev")

SOURCE_SCHEMA = "kafka_sim"

@dlt.table(
    name="orders_raw",
    comment="Incremental ingestion pipeline reading from a streaming Delta source table orders_raw."
)
def orders_raw() -> DataFrame:
    return (
        spark.readStream.format("delta")
        .table(f"{CATALOG}.{SOURCE_SCHEMA}.orders_events")
    )

@dlt.table(
    name="customers_raw",
    comment="Incremental ingestion pipeline reading from a streaming Delta source table customers_raw."
)
def customers_raw() -> DataFrame:
    return (
        spark.readStream.format("delta")
        .table(f"{CATALOG}.{SOURCE_SCHEMA}.customers_events")
    )

@dlt.table(
    name="products_raw",
    comment="Incremental ingestion pipeline reading from a streaming Delta source table products_raw."
)
def products_raw() -> DataFrame:
    return (
        spark.readStream.format("delta")
        .table(f"{CATALOG}.{SOURCE_SCHEMA}.products_events")
    )