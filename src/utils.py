import re
from typing import Dict, Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


_CAMEL_RX = re.compile(r"[^a-zA-Z0-9]+")

def to_camel_case(name: str) -> str:
    """
    Convert column names to lowerCamelCase.
    Examples:
      "release_year" -> "releaseYear"
      "IMDB Score"   -> "imdbScore"
      "age_certification" -> "ageCertification"
    """
    if name is None:
        return name
    raw = name.strip()
    if raw == "":
        return raw
    parts = [p for p in _CAMEL_RX.split(raw) if p]
    if not parts:
        return raw

    first = parts[0].lower()
    rest = [p[:1].upper() + p[1:].lower() for p in parts[1:]]
    return "".join([first] + rest)


def standardize_columns(df: DataFrame, rename_map: Optional[Dict[str, str]] = None) -> DataFrame:
    """
    1) Optionally rename known columns using rename_map (case-insensitive match)
    2) Convert all columns to camelCase
    """
    rename_map = rename_map or {}

    # Build case-insensitive map
    lower_map = {k.lower(): v for k, v in rename_map.items()}

    cols = df.columns
    exprs = []
    for c in cols:
        target = lower_map.get(c.lower(), c)
        exprs.append(F.col(c).alias(to_camel_case(target)))
    return df.select(*exprs)


def add_file_year_from_path(df: DataFrame, path_col: str = "_sourceFile") -> DataFrame:
    """
    Derive fileYear from source file path. Looks for /YYYY/ or YYYY in filename.
    """
    # Try /YYYY/ first, then fallback to YYYY anywhere
    year = F.regexp_extract(F.col(path_col), r"/(19|20)\d{2}/", 0)
    year = F.regexp_extract(F.col(path_col), r"(19|20)\d{2}", 0).when(F.length(year) == 0, year)

    # Better: extract numeric 4-digit year
    year2 = F.regexp_extract(F.col(path_col), r"(19|20)\d{2}", 0)
    return df.withColumn("fileYear", year2.cast("int"))


def safe_int(col):
    return F.when(F.col(col).isNull() | (F.col(col) == ""), None).otherwise(F.col(col).cast("int"))


def safe_double(col):
    return F.when(F.col(col).isNull() | (F.col(col) == ""), None).otherwise(F.col(col).cast("double"))


def trim_string_cols(df: DataFrame) -> DataFrame:
    out = df
    for c, t in df.dtypes:
        if t == "string":
            out = out.withColumn(c, F.trim(F.col(c)))
    return out


def drop_all_null_key_rows(df: DataFrame, keys: Iterable[str]) -> DataFrame:
    cond = None
    for k in keys:
        this = F.col(k).isNotNull() & (F.col(k) != "")
        cond = this if cond is None else (cond & this)
    return df.filter(cond) if cond is not None else df


def dedupe_by_window(df: DataFrame, partition_cols: List[str], order_cols: List[Tuple[str, str]]) -> DataFrame:
    """
    Dedupe keeping the first row within partition by ordering.
    order_cols: list of (colName, 'asc'|'desc')
    """
    order_exprs = []
    for c, direction in order_cols:
        order_exprs.append(F.col(c).asc_nulls_last() if direction.lower() == "asc" else F.col(c).desc_nulls_last())
    w = Window.partitionBy(*partition_cols).orderBy(*order_exprs)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def parse_first_director(director_col: str = "director"):
    """
    Netflix director column often contains comma-separated names.
    """
    return F.when(
        F.col(director_col).isNull() | (F.col(director_col) == ""),
        None
    ).otherwise(F.split(F.col(director_col), r"\s*,\s*").getItem(0))


def ensure_db(spark, db_name: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
