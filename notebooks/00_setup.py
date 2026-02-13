# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Creates databases and sets common Spark configs.

from src.config import Databases

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

for db in [Databases().bronze, Databases().silver, Databases().gold]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

print("Setup complete.")
