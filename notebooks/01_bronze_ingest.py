# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingest
# MAGIC Reads raw CSV(s) from DBFS and writes Delta Bronze tables.

from src.config import Paths, Databases, Tables, PipelineConfig
from src.bronze_ingest import run_bronze

paths = Paths()
dbs = Databases()
tables = Tables()
cfg = PipelineConfig(full_rebuild=True)  # set False for incremental append

run_bronze(spark, paths, dbs, tables, cfg)

display(spark.table(f"{dbs.bronze}.{tables.bronze_netflix}").limit(10))
display(spark.table(f"{dbs.bronze}.{tables.bronze_imdb}").limit(10))
