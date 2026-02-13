-- Titles per year
SELECT releaseYear, COUNT(*) AS totalTitles
FROM movies_gold.movies
GROUP BY releaseYear
ORDER BY releaseYear;

-- Top directors
SELECT director, COUNT(*) AS totalMovies
FROM movies_gold.movies
WHERE director IS NOT NULL AND director <> ''
GROUP BY director
ORDER BY totalMovies DESC
LIMIT 25;

-- Most common age certifications
SELECT ageCertification, COUNT(*) AS total
FROM movies_gold.movies
WHERE ageCertification IS NOT NULL AND ageCertification <> ''
GROUP BY ageCertification
ORDER BY total DESC
LIMIT 25;
