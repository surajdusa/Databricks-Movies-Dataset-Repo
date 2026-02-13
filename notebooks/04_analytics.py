# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Analytics Queries
# MAGIC Required outputs:
# MAGIC - titles per year
# MAGIC - top directors
# MAGIC - most common age certifications

from src.config import Databases, Tables

dbs = Databases()
tables = Tables()

movies = spark.table(f"{dbs.gold}.{tables.gold_movies}")
movies.createOrReplaceTempView("movies")

print("Titles per year")
display(spark.sql("""
SELECT releaseYear, COUNT(*) AS totalTitles
FROM movies
GROUP BY releaseYear
ORDER BY releaseYear
"""))

print("Top directors by content production")
display(spark.sql("""
SELECT director, COUNT(*) AS totalMovies
FROM movies
WHERE director IS NOT NULL AND director <> ''
GROUP BY director
ORDER BY totalMovies DESC, director ASC
LIMIT 25
"""))

print("Most common age certifications")
display(spark.sql("""
SELECT ageCertification, COUNT(*) AS total
FROM movies
WHERE ageCertification IS NOT NULL AND ageCertification <> ''
GROUP BY ageCertification
ORDER BY total DESC, ageCertification ASC
LIMIT 25
"""))
