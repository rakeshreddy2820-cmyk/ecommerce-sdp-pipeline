# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Setup: Create Catalogs, Schemas, and Source Tables

# COMMAND ----------
dbutils.widgets.text("catalog", "dev")
CATALOG = dbutils.widgets.get("catalog")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.kafka_sim")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

print(f"Catalog '{CATALOG}' and schemas created ✓")

# COMMAND ----------
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CATALOG}.kafka_sim.orders_events (
    event_id        STRING,
    event_type      STRING,
    event_timestamp TIMESTAMP,
    order_id        STRING,
    customer_id     STRING,
    product_id      STRING,
    amount          DOUBLE,
    status          STRING,
    items           STRING
  )
  USING DELTA
""")
print("orders_events created ✓")

# COMMAND ----------
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CATALOG}.kafka_sim.customers_events (
    event_id        STRING,
    event_type      STRING,
    event_timestamp TIMESTAMP,
    customer_id     STRING,
    name            STRING,
    email           STRING,
    city            STRING,
    country         STRING
  )
  USING DELTA
""")
print("customers_events created ✓")

# COMMAND ----------
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CATALOG}.kafka_sim.products_events (
    event_id        STRING,
    event_type      STRING,
    event_timestamp TIMESTAMP,
    product_id      STRING,
    name            STRING,
    category        STRING,
    price           DOUBLE,
    stock           INT
  )
  USING DELTA
""")
print("products_events created ✓")

# COMMAND ----------
display(spark.sql(f"SHOW TABLES IN {CATALOG}.kafka_sim"))