"""All eight exploration tasks. Language facts are collected by enrich.py."""
import argparse
from pathlib import Path

from prepare import register, save, session

VIEWS = {
    "movies": """SELECT t.*, r.averageRating, r.numVotes FROM titles t
        LEFT JOIN ratings r USING(tconst) WHERE titleType='movie'""",
    "features": "SELECT * FROM movies WHERE runtimeMinutes >= 40",
    "film_genres": "SELECT f.*, explode(split(genres, ',')) genre FROM features f",
    "cast_movies": """SELECT DISTINCT p.nconst, m.tconst, m.startYear, m.averageRating
        FROM principals p JOIN movies m USING(tconst) WHERE category IN ('actor','actress')""",
    "directed": """SELECT DISTINCT tconst, explode(split(directors, ',')) nconst FROM crew""",
    "roles": """SELECT tconst,nconst,'director' role FROM directed UNION
        SELECT tconst,explode(split(writers, ',')), 'writer' FROM crew UNION
        SELECT tconst,nconst,'actor' FROM cast_movies""",
    "genre_counts": """SELECT startYear, genre, count(*) films, sum(coalesce(numVotes,0)) votes
        FROM (SELECT startYear,numVotes,explode(split(genres, ',')) genre FROM movies)
        WHERE startYear BETWEEN 1980 AND 2025 GROUP BY startYear,genre""",
    "genre_ranks": """SELECT *, row_number() OVER(PARTITION BY startYear ORDER BY votes DESC,genre) position
        FROM genre_counts""",
}
QUERIES = {
    "runtime_outliers": """SELECT * FROM (SELECT tconst,primaryTitle,genre,runtimeMinutes,
        avg(runtimeMinutes) OVER(PARTITION BY genre) mean_minutes,
        stddev_pop(runtimeMinutes) OVER(PARTITION BY genre) sd FROM film_genres)
        WHERE sd>0 AND abs(runtimeMinutes-mean_minutes)>2*sd""",
    "annual_top_three": "SELECT * FROM genre_ranks WHERE position<=3",
    "annual_comparison": """SELECT startYear, sum(CASE WHEN position<=3 THEN votes ELSE 0 END) top_votes,
        sum(CASE WHEN position>3 THEN votes ELSE 0 END) other_votes,
        sum(CASE WHEN position<=3 THEN votes ELSE 0 END)/nullif(sum(votes),0) top_share
        FROM genre_ranks GROUP BY startYear""",
    "directors": """WITH works AS (SELECT * FROM directed JOIN movies USING(tconst)),
        eligible AS (SELECT nconst, max(CASE WHEN averageRating>=8 THEN tconst END) hit,
        count_if(averageRating<8) other_rated, avg(CASE WHEN averageRating<8 THEN averageRating END) other_mean
        FROM works GROUP BY nconst HAVING count_if(averageRating>=8)=1 AND other_rated>=2 AND other_mean<=6)
        SELECT e.*,n.primaryName,m.primaryTitle FROM eligible e JOIN names n USING(nconst)
        JOIN movies m ON m.tconst=e.hit""",
    "tv_extremes": """WITH ep AS (SELECT e.*,r.averageRating FROM episodes e LEFT JOIN ratings r USING(tconst)
        WHERE seasonNumber>0), seasons AS (SELECT parentTconst,seasonNumber,count(*) size FROM ep GROUP BY 1,2),
        series AS (SELECT parentTconst,max(seasonNumber) last_season FROM seasons GROUP BY 1
        HAVING count_if(size>=10)>=3), ranked AS (SELECT parentTconst,last_season,
        min(averageRating) worst,max(averageRating) best,
        min(CASE WHEN seasonNumber=last_season THEN averageRating END) final_low,
        max(CASE WHEN seasonNumber<=2 THEN averageRating END) early_high FROM ep JOIN series USING(parentTconst)
        GROUP BY 1,2) SELECT ranked.*,t.primaryTitle FROM ranked JOIN titles t ON t.tconst=parentTconst
        WHERE worst=final_low AND best=early_high AND best>worst AND titleType='tvSeries'""",
    "languages": """WITH l AS (SELECT DISTINCT c.nconst,x.language FROM cast_movies c JOIN languages_source x
        USING(tconst) JOIN ratings r USING(tconst) WHERE r.averageRating>=7 AND r.numVotes>=1000 AND x.language IS NOT NULL)
        SELECT nconst,primaryName,sort_array(collect_set(language)) languages,count(*) language_count
        FROM l JOIN names USING(nconst) GROUP BY nconst,primaryName HAVING count(*)>=2""",
    "early_peaks": """WITH careers AS (SELECT nconst,min(startYear) debut,max(startYear) latest,count(*) films
        FROM cast_movies WHERE startYear IS NOT NULL GROUP BY 1 HAVING latest-debut>=20 AND films>=15),
        split AS (SELECT *,startYear<=debut+(latest-debut)/3 early FROM careers JOIN cast_movies USING(nconst)),
        scores AS (SELECT nconst,debut,latest,films,max(CASE WHEN early THEN averageRating END) early_peak,
        max(CASE WHEN NOT early THEN averageRating END) later_peak,avg(CASE WHEN early THEN averageRating END) early_mean,
        avg(CASE WHEN NOT early THEN averageRating END) later_mean FROM split GROUP BY 1,2,3,4)
        SELECT scores.*,primaryName FROM scores JOIN names USING(nconst)
        WHERE early_peak>later_peak AND early_mean>later_mean""",
    "multi_talented": """WITH people AS (SELECT nconst FROM roles JOIN movies USING(tconst)
        GROUP BY nconst HAVING count(DISTINCT role)>=2), credits AS (SELECT nconst,tconst,count(DISTINCT role) roles_on_film
        FROM roles JOIN people USING(nconst) GROUP BY 1,2)
        SELECT nconst,primaryName,count_if(roles_on_film>1) multi_films,count_if(roles_on_film=1) single_films,
        avg(CASE WHEN roles_on_film>1 THEN averageRating END) multi_mean,
        avg(CASE WHEN roles_on_film=1 THEN averageRating END) single_mean
        FROM credits JOIN movies USING(tconst) JOIN names USING(nconst) GROUP BY 1,2""",
}


def views(spark, root):
    register(spark, root)
    for name, sql in VIEWS.items():
        spark.sql(sql).createOrReplaceTempView(name)


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("--root", type=Path, default=Path("work"))
    cli.add_argument("--stage", choices=["all", "core", "languages"], default="all")
    args = cli.parse_args()
    spark = session()
    views(spark, args.root)
    if args.stage != "core":
        spark.read.parquet(str(args.root / "enriched/languages")).createOrReplaceTempView("languages_source")
    for name, sql in QUERIES.items():
        if (args.stage == "core" and name == "languages") or (args.stage == "languages" and name != "languages"):
            continue
        result = spark.sql(sql)
        save(result, args.root / "results" / name)
        if name == "languages":
            save(result.where("language_count>=3"), args.root / "results/languages_three")
