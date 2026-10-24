# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Setup: Create Catalogs, Schemas, and Source Tables

# COMMAND ----------
# Create catalog and all schemas
spark.sql("CREATE CATALOG IF NOT EXISTS dev")

spark.sql("CREATE SCHEMA IF NOT EXISTS dev.kafka_sim")
spark.sql("CREATE SCHEMA IF NOT EXISTS dev.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS dev.gold")

print("Catalog and schemas created ✓")

# COMMAND ----------
# Orders source table (simulates Kafka orders topic)
spark.sql("""
  CREATE TABLE IF NOT EXISTS dev.kafka_sim.orders_events (
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
# Customers source table (simulates Kafka customers topic)
spark.sql("""
  CREATE TABLE IF NOT EXISTS dev.kafka_sim.customers_events (
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
# Products source table (simulates Kafka products topic)
spark.sql("""
  CREATE TABLE IF NOT EXISTS dev.kafka_sim.products_events (
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
# Verify everything exists
display(spark.sql("SHOW TABLES IN dev.kafka_sim"))