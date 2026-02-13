from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .config import Paths, Databases, Tables, PipelineConfig
from .utils import dedupe_by_window, ensure_db


def load_silver(spark, db: str, table: str) -> DataFrame:
    return spark.table(f"{db}.{table}")


def select_best_imdb(imdb: DataFrame) -> DataFrame:
    """
    For duplicates with same (title, releaseYear), choose highest imdbRating,
    then highest imdbVotes.
    """
    # Ensure required cols exist (best-effort)
    if "imdbRating" not in imdb.columns:
        imdb = imdb.withColumn("imdbRating", F.lit(None).cast("double"))
    if "imdbVotes" not in imdb.columns:
        imdb = imdb.withColumn("imdbVotes", F.lit(None).cast("int"))

    return dedupe_by_window(
        imdb,
        partition_cols=["title", "releaseYear"],
        order_cols=[("imdbRating", "desc"), ("imdbVotes", "desc")]
    )


def build_gold_movies(netflix: DataFrame, imdb_best: DataFrame) -> DataFrame:
    """
    Gold rules (assessment):
    - Start from Netflix movies only (must include all Netflix movies, exclude non-Netflix)
    - One row per title + releaseYear
    - All other columns from Netflix
    - imdbRating + imdbVotes from IMDB if exists else Netflix (if Netflix has them)
    - director: earliest person_id if multiple (not always available); else primaryDirector
    """
    n = netflix
    # Filter Netflix to movies only
    if "type" in n.columns:
        n = n.filter(F.col("type") == F.lit("Movie"))

    # Ensure keys exist
    if "releaseYear" not in n.columns:
        raise ValueError("Netflix dataset must contain releaseYear for gold conformance.")

    # Netflix may already contain imdbScore/imdbVotes; standardize to imdbRating/imdbVotes
    if "imdbScore" in n.columns and "imdbRating" not in n.columns:
        n = n.withColumnRenamed("imdbScore", "imdbRating")
    if "imdbVotes" not in n.columns:
        n = n.withColumn("imdbVotes", F.lit(None).cast("int"))
    if "imdbRating" not in n.columns:
        n = n.withColumn("imdbRating", F.lit(None).cast("double"))

    # Deduplicate Netflix movies on (title, releaseYear) deterministically:
    # prefer non-null imdbRating, then longer duration if parseable, then stable by showId
    order_cols = []
    order_cols.append(("imdbRating", "desc"))
    if "duration" in n.columns:
        # Extract minutes if present like "95 min"
        n = n.withColumn("_durationMinutes", F.regexp_extract(F.col("duration"), r"(\d+)", 1).cast("int"))
        order_cols.append(("_durationMinutes", "desc"))
    if "showId" in n.columns:
        order_cols.append(("showId", "desc"))

    n_best = dedupe_by_window(n, ["title", "releaseYear"], order_cols)

    # Join enrichment
    j = n_best.alias("n").join(imdb_best.alias("i"), on=["title", "releaseYear"], how="left")

    # Rating/votes selection rule:
    # Use IMDB if exists; else Netflix
    j = (
        j.withColumn("finalImdbRating", F.coalesce(F.col("i.imdbRating"), F.col("n.imdbRating")))
         .withColumn("finalImdbVotes", F.coalesce(F.col("i.imdbVotes"), F.col("n.imdbVotes")))
    )

    # Director selection:
    # If IMDB contains person_id-based director structure, pick min(personId)
    # Otherwise, coalesce to Netflix primaryDirector.
    if "directorPersonId" in imdb_best.columns and "directorName" in imdb_best.columns:
        # If multiple rows existed, imdb_best already reduced, so just use it.
        director_expr = F.col("i.directorName")
    else:
        director_expr = F.coalesce(F.col("i.primaryDirector"), F.col("n.primaryDirector"))

    # Age certification:
    # Prefer Netflix rating (often "PG-13", "TV-MA"), else IMDB ageCertification if present
    age_expr = None
    if "rating" in n_best.columns:
        age_expr = F.col("n.rating")
    elif "ageCertification" in imdb_best.columns:
        age_expr = F.col("i.ageCertification")
    else:
        age_expr = F.lit(None).cast("string")

    # Runtime:
    # Prefer IMDB runtimeMinutes if available else parse from Netflix duration
    runtime_expr = None
    if "runtimeMinutes" in imdb_best.columns:
        runtime_expr = F.col("i.runtimeMinutes")
    elif "_durationMinutes" in j.columns:
        runtime_expr = F.col("n._durationMinutes")
    else:
        runtime_expr = F.lit(None).cast("int")

    # Build final projection: "All other columns from Netflix" + enriched rating/votes
    # Keep it minimal but compliant for filtering
    keep_cols = []
    for c in n_best.columns:
        if c.startswith("_"):
            continue
        keep_cols.append(F.col(f"n.{c}").alias(c))

    out = (
        j.select(
            *keep_cols,
            director_expr.alias("director"),
            age_expr.alias("ageCertification"),
            runtime_expr.alias("runtimeMinutes"),
            F.col("finalImdbRating").alias("imdbRating"),
            F.col("finalImdbVotes").cast("int").alias("imdbVotes"),
        )
    )

    # Enforce one row per title+releaseYear
    out = dedupe_by_window(out, ["title", "releaseYear"], [("imdbRating", "desc"), ("imdbVotes", "desc")])
    return out


def write_gold(spark, df: DataFrame, db: str, table: str, storage_path: str, full_rebuild: bool):
    ensure_db(spark, db)
    mode = "overwrite" if full_rebuild else "append"
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true" if full_rebuild else "false")
        .save(storage_path)
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING DELTA LOCATION '{storage_path}'")


def run_gold(spark, paths: Paths, dbs: Databases, tables: Tables, cfg: PipelineConfig):
    netflix = load_silver(spark, dbs.silver, tables.silver_netflix)
    imdb = load_silver(spark, dbs.silver, tables.silver_imdb)

    imdb_best = select_best_imdb(imdb)
    gold = build_gold_movies(netflix, imdb_best)

    write_gold(
        spark,
        gold,
        dbs.gold,
        tables.gold_movies,
        f"{paths.gold_root}/{tables.gold_movies}",
        cfg.full_rebuild,
    )
