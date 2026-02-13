# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Conformed Movies Table
# MAGIC Builds one-row-per-movie table from Netflix + IMDB.

from src.config import Paths, Databases, Tables, PipelineConfig
from src.gold_conform import run_gold

paths = Paths()
dbs = Databases()
tables = Tables()
cfg = PipelineConfig(full_rebuild=True)

run_gold(spark, paths, dbs, tables, cfg)

display(spark.table(f"{dbs.gold}.{tables.gold_movies}").limit(25))
