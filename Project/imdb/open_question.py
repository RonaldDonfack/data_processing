"""Within-director comparison of first and repeat actor collaborations."""
import argparse
from pathlib import Path

from explore import views
from prepare import save, session

QUERY = """
WITH pairs AS (
    SELECT d.nconst director,c.nconst actor,m.tconst,m.startYear,m.averageRating
    FROM directed d JOIN cast_movies c USING(tconst) JOIN movies m USING(tconst)
    WHERE d.nconst<>c.nconst AND m.startYear IS NOT NULL
), history AS (
    SELECT *,min(startYear) OVER(PARTITION BY director,actor) first_year FROM pairs
), films AS (
    SELECT director,tconst,max(averageRating) rating,max(int(startYear>first_year)) repeat_cast
    FROM history GROUP BY director,tconst
), contrasts AS (
    SELECT director,count_if(repeat_cast=1) repeat_films,count_if(repeat_cast=0) first_films,
    avg(CASE WHEN repeat_cast=1 THEN rating END) repeat_mean,
    avg(CASE WHEN repeat_cast=0 THEN rating END) first_mean
    FROM films WHERE rating IS NOT NULL GROUP BY director HAVING repeat_films>=2 AND first_films>=2
)
SELECT director,primaryName,repeat_films,first_films,repeat_mean,first_mean,repeat_mean-first_mean difference
FROM contrasts JOIN names ON director=nconst
"""

if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("--root", type=Path, default=Path("work"))
    args = cli.parse_args()
    spark = session()
    views(spark, args.root)
    results = spark.sql(QUERY)
    save(results, args.root / "results/repeat_collaborations")
    results.createOrReplaceTempView("contrasts")
    save(spark.sql("""SELECT count(*) directors,avg(difference) mean_within_director_difference,
        percentile_approx(difference,0.5) median_difference FROM contrasts"""), args.root / "results/repeat_summary")
