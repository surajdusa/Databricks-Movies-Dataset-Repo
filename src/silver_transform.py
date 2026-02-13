from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import Paths, Databases, Tables, PipelineConfig
from .utils import (
    add_file_year_from_path,
    dedupe_by_window,
    drop_all_null_key_rows,
    ensure_db,
    parse_first_director,
    safe_double,
    safe_int,
    standardize_columns,
    trim_string_cols,
)


def load_bronze(spark, db: str, table: str) -> DataFrame:
    return spark.table(f"{db}.{table}")


def clean_netflix(df: DataFrame) -> DataFrame:
    """
    Expected Netflix columns (commonly):
    showId, type, title, director, cast, country, dateAdded,
    releaseYear, rating, duration, listedIn, description
    """
    d = trim_string_cols(df)

    # Normalize key columns
    if "releaseYear" not in d.columns and "release_year" in df.columns:
        d = standardize_columns(d, {"release_year": "releaseYear"})

    # Cast year
    if "releaseYear" in d.columns:
        d = d.withColumn("releaseYear", safe_int("releaseYear"))

    # Keep only Movies for downstream gold, but keep full netflix in silver
    # (we filter later in gold to be explicit)

    # Parse primary director (first in list)
    if "director" in d.columns:
        d = d.withColumn("primaryDirector", parse_first_director("director"))
    else:
        d = d.withColumn("primaryDirector", F.lit(None).cast("string"))

    # Cast Netflix rating/votes if exist (some variants include imdb_score / imdb_votes)
    if "imdbScore" in d.columns:
        d = d.withColumn("imdbScore", safe_double("imdbScore"))
    if "imdbVotes" in d.columns:
        d = d.withColumn("imdbVotes", safe_int("imdbVotes"))

    # Drop null titles
    d = drop_all_null_key_rows(d, ["title"])

    # Dedupe Netflix on (title, releaseYear, type) with deterministic tie-breaks
    # Prefer rows with non-null director, then latest dateAdded if present, then max showId.
    order_cols = []
    if "director" in d.columns:
        order_cols.append(("director", "desc"))
    if "dateAdded" in d.columns:
        order_cols.append(("dateAdded", "desc"))
    if "showId" in d.columns:
        order_cols.append(("showId", "desc"))

    part_cols = [c for c in ["title", "releaseYear", "type"] if c in d.columns]
    if part_cols and order_cols:
        d = dedupe_by_window(d, part_cols, order_cols)

    return d


def clean_imdb(df: DataFrame) -> DataFrame:
    """
    IMDB dataset columns vary a lot by Kaggle source.
    We handle common patterns:
      - title, year / releaseYear
      - imdbRating / rating / averageRating
      - numVotes / votes
      - runtimeMinutes / runtime
      - certification / ageCertification
      - directors (string) or directorName
    """
    d = trim_string_cols(df)

    # Add fileYear for lineage (required)
    if "_sourceFile" in d.columns:
        d = add_file_year_from_path(d, "_sourceFile")
    else:
        d = d.withColumn("fileYear", F.lit(None).cast("int"))

    # Standardize likely column names into a consistent set
    rename_map = {
        "year": "releaseYear",
        "release_year": "releaseYear",
        "startYear": "releaseYear",
        "imdb_rating": "imdbRating",
        "rating": "imdbRating",
        "averageRating": "imdbRating",
        "numVotes": "imdbVotes",
        "votes": "imdbVotes",
        "runtime": "runtimeMinutes",
        "runtime_min": "runtimeMinutes",
        "certificate": "ageCertification",
        "certification": "ageCertification",
    }
    d = standardize_columns(d, rename_map)

    if "releaseYear" in d.columns:
        d = d.withColumn("releaseYear", safe_int("releaseYear"))
    if "imdbRating" in d.columns:
        d = d.withColumn("imdbRating", safe_double("imdbRating"))
    if "imdbVotes" in d.columns:
        d = d.withColumn("imdbVotes", safe_int("imdbVotes"))
    if "runtimeMinutes" in d.columns:
        d = d.withColumn("runtimeMinutes", safe_int("runtimeMinutes"))

    # Drop null titles and years (we need title/year for joining)
    d = drop_all_null_key_rows(d, ["title", "releaseYear"])

    # If IMDB has directors string, derive a primaryDirector name as first token
    if "directors" in d.columns and "primaryDirector" not in d.columns:
        d = d.withColumn("primaryDirector", F.split(F.col("directors"), r"\s*,\s*").getItem(0))
    elif "director" in d.columns and "primaryDirector" not in d.columns:
        d = d.withColumn("primaryDirector", F.split(F.col("director"), r"\s*,\s*").getItem(0))
    elif "primaryDirector" not in d.columns:
        d = d.withColumn("primaryDirector", F.lit(None).cast("string"))

    return d


def write_silver_table(spark, df: DataFrame, db: str, table: str, storage_path: str, full_rebuild: bool, merge_schema: bool):
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


def run_silver(spark, paths: Paths, dbs: Databases, tables: Tables, cfg: PipelineConfig):
    bronze_netflix = load_bronze(spark, dbs.bronze, tables.bronze_netflix)
    bronze_imdb = load_bronze(spark, dbs.bronze, tables.bronze_imdb)

    silver_netflix = clean_netflix(bronze_netflix)
    silver_imdb = clean_imdb(bronze_imdb)

    write_silver_table(
        spark, silver_netflix,
        dbs.silver, tables.silver_netflix,
        f"{paths.silver_root}/{tables.silver_netflix}",
        cfg.full_rebuild, cfg.enable_schema_merge
    )
    write_silver_table(
        spark, silver_imdb,
        dbs.silver, tables.silver_imdb,
        f"{paths.silver_root}/{tables.silver_imdb}",
        cfg.full_rebuild, cfg.enable_schema_merge
    )
