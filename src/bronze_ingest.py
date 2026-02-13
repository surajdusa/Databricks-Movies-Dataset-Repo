from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import Paths, Databases, Tables, PipelineConfig
from .utils import ensure_db, standardize_columns


def read_netflix_raw(spark, paths: Paths) -> DataFrame:
    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", "\"")
        .csv(paths.netflix_csv)
        .withColumn("_sourceFile", F.input_file_name())
    )
    # Standardize early so downstream is stable
    return standardize_columns(df)


def read_imdb_raw_years(spark, paths: Paths, cfg: PipelineConfig) -> DataFrame:
    year_paths: List[str] = []
    for y in range(cfg.imdb_year_start, cfg.imdb_year_end + 1):
        year_paths.append(f"{paths.imdb_root}/{y}/*.csv")

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", "\"")
        .csv(year_paths)
        .withColumn("_sourceFile", F.input_file_name())
    )
    return standardize_columns(df)


def write_bronze_table(spark, df: DataFrame, db: str, table: str, storage_path: str, full_rebuild: bool, merge_schema: bool):
    ensure_db(spark, db)
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true" if merge_schema else "false")

    mode = "overwrite" if full_rebuild else "append"
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true" if full_rebuild else "false")
        .option("mergeSchema", "true" if merge_schema else "false")
        .save(storage_path)
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING DELTA LOCATION '{storage_path}'")


def run_bronze(spark, paths: Paths, dbs: Databases, tables: Tables, cfg: PipelineConfig):
    # Netflix
    netflix = read_netflix_raw(spark, paths)
    write_bronze_table(
        spark,
        netflix,
        dbs.bronze,
        tables.bronze_netflix,
        f"{paths.bronze_root}/{tables.bronze_netflix}",
        cfg.full_rebuild,
        cfg.enable_schema_merge,
    )

    # IMDB (2010-2025)
    imdb = read_imdb_raw_years(spark, paths, cfg)
    write_bronze_table(
        spark,
        imdb,
        dbs.bronze,
        tables.bronze_imdb,
        f"{paths.bronze_root}/{tables.bronze_imdb}",
        cfg.full_rebuild,
        cfg.enable_schema_merge,
    )
