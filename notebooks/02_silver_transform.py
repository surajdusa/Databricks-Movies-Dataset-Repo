# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Transform
# MAGIC Cleans, standardizes, deduplicates, and writes Silver tables.

from src.config import Paths, Databases, Tables, PipelineConfig
from src.silver_transform import run_silver

paths = Paths()
dbs = Databases()
tables = Tables()
cfg = PipelineConfig(full_rebuild=True)

run_silver(spark, paths, dbs, tables, cfg)

display(spark.table(f"{dbs.silver}.{tables.silver_netflix}").limit(10))
display(spark.table(f"{dbs.silver}.{tables.silver_imdb}").limit(10))
