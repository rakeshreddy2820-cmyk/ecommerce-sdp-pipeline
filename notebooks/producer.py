# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Producer: Simulate Kafka Events
# MAGIC Run this notebook WHILE the SDP pipeline is running to feed it data.

# COMMAND ----------
import uuid
import random
from datetime import datetime
from pyspark.sql import Row

# COMMAND ----------
# Sample data pools
customer_ids = [f"cust_{i:03d}" for i in range(1, 21)]
product_ids  = [f"prod_{i:03d}" for i in range(1, 11)]
categories   = ["Electronics", "Clothing", "Books", "Home", "Sports"]
cities       = ["London", "Mumbai", "New York", "Berlin", "Tokyo"]
countries    = ["UK", "IN", "US", "DE", "JP"]
statuses     = ["processing", "shipped", "delivered"]

# COMMAND ----------
def insert_order_event():
    event_type = random.choice(["new_order", "new_order", "update_status", "cancel_order"])
    row = Row(
        event_id        = str(uuid.uuid4()),
        event_type      = event_type,
        event_timestamp = datetime.now(),
        order_id        = f"ord_{uuid.uuid4().hex[:8]}",
        customer_id     = random.choice(customer_ids),
        product_id      = random.choice(product_ids),
        amount          = round(random.uniform(10.0, 500.0), 2),
        status          = random.choice(statuses),
        items           = f'[{{"product_id": "{random.choice(product_ids)}", "qty": {random.randint(1,5)}, "unit_price": {round(random.uniform(10.0, 200.0), 2)}}}]'
    )
    spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable("dev.kafka_sim.orders_events")

# COMMAND ----------
def insert_customer_event():
    i = random.randint(1, 20)
    event_type = random.choice(["register", "update_profile"])
    row = Row(
        event_id        = str(uuid.uuid4()),
        event_type      = event_type,
        event_timestamp = datetime.now(),
        customer_id     = f"cust_{i:03d}",
        name            = random.choice(["Alice", "Bob", "Carlos", "Diana", "Ravi", "Sara"]),
        email           = f"user{i}@example.com",
        city            = random.choice(cities),
        country         = random.choice(countries)
    )
    spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable("dev.kafka_sim.customers_events")

# COMMAND ----------
def insert_product_event():
    i = random.randint(1, 10)
    event_type = random.choice(["add_product", "update_product", "update_product", "remove_product"])
    row = Row(
        event_id        = str(uuid.uuid4()),
        event_type      = event_type,
        event_timestamp = datetime.now(),
        product_id      = f"prod_{i:03d}",
        name            = f"Product {i}",
        category        = random.choice(categories),
        price           = round(random.uniform(5.0, 300.0), 2),
        stock           = random.randint(0, 500)
    )
    spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable("dev.kafka_sim.products_events")

# COMMAND ----------
# Run 50 rounds of events — re-run this cell to keep feeding data
for i in range(50):
    insert_order_event()
    insert_customer_event()
    insert_product_event()

print("50 rounds of events inserted ✓")

# COMMAND ----------
# Verify row counts
display(spark.sql("""
  SELECT 'orders'    AS topic, COUNT(*) AS rows FROM dev.kafka_sim.orders_events
  UNION ALL
  SELECT 'customers' AS topic, COUNT(*) AS rows FROM dev.kafka_sim.customers_events
  UNION ALL
  SELECT 'products'  AS topic, COUNT(*) AS rows FROM dev.kafka_sim.products_events
"""))