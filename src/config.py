from dataclasses import dataclass

@dataclass(frozen=True)
class Paths:
    # Input data (DBFS)
    netflix_csv: str = "dbfs:/FileStore/movies/netflix/netflix_titles.csv"
    imdb_root: str = "dbfs:/FileStore/movies/imdb"  # expects /2010 ... /2025 subfolders

    # Delta storage locations
    delta_root: str = "dbfs:/FileStore/delta/movies"

    @property
    def bronze_root(self) -> str:
        return f"{self.delta_root}/bronze"

    @property
    def silver_root(self) -> str:
        return f"{self.delta_root}/silver"

    @property
    def gold_root(self) -> str:
        return f"{self.delta_root}/gold"


@dataclass(frozen=True)
class Databases:
    bronze: str = "movies_bronze"
    silver: str = "movies_silver"
    gold: str = "movies_gold"


@dataclass(frozen=True)
class Tables:
    # Bronze
    bronze_netflix: str = "netflixTitles"
    bronze_imdb: str = "imdbTitles"
    # Silver
    silver_netflix: str = "netflixTitles"
    silver_imdb: str = "imdbTitles"
    # Gold
    gold_movies: str = "movies"


@dataclass(frozen=True)
class PipelineConfig:
    # Toggle between full rebuild vs incremental merge-upsert patterns
    full_rebuild: bool = True

    # Range required by assessment
    imdb_year_start: int = 2010
    imdb_year_end: int = 2025

    # Keys
    movie_key_cols: tuple = ("title", "releaseYear")

    # Write options
    enable_schema_merge: bool = True
