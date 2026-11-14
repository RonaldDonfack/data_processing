"""Download the seven IMDb tables, audit raw cells and produce typed Parquet."""
import argparse
import gzip
import hashlib
import json
import os
import shutil
from functools import reduce
from operator import or_
import urllib.request
from pyspark import StorageLevel
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

TABLES = {
    "titles": ("title.basics", "tconst titleType primaryTitle originalTitle isAdult startYear endYear runtimeMinutes genres"),
    "names": ("name.basics", "nconst primaryName birthYear deathYear primaryProfession knownForTitles"),
    "akas": ("title.akas", "titleId ordering title region language types attributes isOriginalTitle"),
    "crew": ("title.crew", "tconst directors writers"),
    "episodes": ("title.episode", "tconst parentTconst seasonNumber episodeNumber"),
    "principals": ("title.principals", "tconst ordering nconst category job characters"),
    "ratings": ("title.ratings", "tconst averageRating numVotes"),
}
NUMBERS = set("isAdult startYear endYear runtimeMinutes birthYear deathYear ordering isOriginalTitle seasonNumber episodeNumber numVotes averageRating".split())
KEYS = {"akas": ["titleId", "ordering"], "principals": ["tconst", "ordering"]}


def download(raw):
    raw.mkdir(parents=True, exist_ok=True)
    manifest = []
    for stem, _ in TABLES.values():
        path = raw / (stem + ".tsv.gz")
        url = "https://datasets.imdbws.com/" + path.name
        if not path.exists():
            tmp = path.with_suffix(".part")
            with urllib.request.urlopen(url, timeout=180) as response, tmp.open("wb") as out:
                shutil.copyfileobj(response, out)
            os.replace(tmp, path)
        with path.open("rb") as data:
            digest = hashlib.file_digest(data, "sha256").hexdigest()
        manifest.append(dict(file=path.name, url=url, sha256=digest))
    (raw / "manifest.json").write_text(json.dumps(manifest, indent=2))


def session():
    return SparkSession.builder.appName("IMDb coursework").config("spark.sql.session.timeZone", "UTC").getOrCreate()


def save(df, path):
    df.write.mode("overwrite").parquet(str(path))


def register(spark, root):
    for name in TABLES:
        spark.read.parquet(str(root / "clean" / name)).createOrReplaceTempView(name)


def prepare(spark, root):
    profile, issues = [], []
    for name, (stem, columns) in TABLES.items():
        # Gzip streams are unsplittable. Expand one table at a time so Spark can
        # parallelize input splits without holding the full TSV in memory.
        tsv = root / "raw" / (stem + ".tsv")
        with gzip.open(root / "raw" / (stem + ".tsv.gz"), "rb") as src, tsv.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        df = (spark.read.option("header", True).option("sep", "\t").option("quote", "")
              .option("mode", "FAILFAST").csv(str(tsv)))
        if df.columns != columns.split():
            raise ValueError(f"Unexpected header for {stem}: {df.columns}")
        df = df.select(*(F.when(~F.trim(F.col(c)).isin("", r"\N"), F.trim(F.col(c))).alias(c) for c in df.columns))
        df = df.repartition(int(spark.conf.get("spark.sql.shuffle.partitions"))).persist(StorageLevel.DISK_ONLY)
        raw_cache = df
        keys = KEYS.get(name, [df.columns[0]])
        numeric = sorted(NUMBERS.intersection(df.columns))
        casts = {c: F.expr(f"try_cast({c} as {'double' if c == 'averageRating' else 'int'})") for c in numeric}
        # Every count() is a Spark action that rescans the table. The cast failures and
        # null keys are therefore folded into the single aggregation that already
        # profiles missing values: one job per table instead of one per check.
        stats = df.agg(
            F.count("*").alias("_rows"),
            *(F.count(c).alias(c) for c in df.columns),
            *(F.count_if(F.col(c).isNotNull() & casts[c].isNull()).alias("_bad_" + c) for c in numeric),
            F.count_if(reduce(or_, (F.col(k).isNull() for k in keys))).alias("_null_keys"),
        ).first()
        total = stats["_rows"]
        for c in df.columns:
            missing = total - stats[c]
            profile.append((stem, c, total, missing, 100 * missing / total if total else None))
        # try_cast turns unparseable values into null instead of failing the job.
        # Those values were present in the raw file, so they are reported as a
        # quality issue rather than silently merged into the missing-value rate.
        for c in numeric:
            issues.append((name, f"non-numeric {c} set to null", stats["_bad_" + c]))
            df = df.withColumn(c, casts[c])
        issues.append((name, "rows with a null key excluded", stats["_null_keys"]))
        df = df.dropna(subset=keys)
        repeated = df.groupBy(*keys).count().filter("count > 1")
        repeated = repeated.persist(StorageLevel.DISK_ONLY)
        # A repeated key cannot be resolved without a tie-breaker, so every row
        # sharing it is excluded. Report rows (sum of counts), not distinct keys,
        # so the figure matches the data actually removed.
        dupes = repeated.agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0]
        issues.append((name, "rows sharing a repeated key excluded", dupes))
        df = df.join(repeated.select(*keys), keys, "left_anti")
        if name == "titles":
            checks = df.agg(F.count_if(F.col("endYear") < F.col("startYear")),
                            F.count_if(F.col("runtimeMinutes") <= 0)).first()
            issues.append((name, "endYear before startYear (endYear set to null)", checks[0]))
            issues.append((name, "non-positive runtimeMinutes set to null", checks[1]))
            # Only the inconsistent attribute is nulled: the title itself stays valid
            # and remains available to every downstream join.
            df = (df.withColumn("endYear", F.when(~(F.col("endYear") < F.col("startYear")) | F.col("startYear").isNull(), F.col("endYear")))
                    .withColumn("runtimeMinutes", F.when(F.col("runtimeMinutes") > 0, F.col("runtimeMinutes"))))
        if name == "ratings":
            # IMDb ratings range from 1.0 to 10.0 and require at least one vote. A row
            # outside that domain carries no usable information, so it is dropped.
            valid = F.col("averageRating").between(1, 10) & (F.col("numVotes") >= 1)
            issues.append((name, "rating outside 1-10 or no votes excluded", df.filter(~valid).count()))
            df = df.filter(valid)
        if name == "crew":
            # directors and writers are comma-separated nconst lists, i.e. foreign keys
            # stored inside a string. posexplode gives one row per (title, position,
            # person); a left_semi join keeps known people; the list is rebuilt in its
            # original order so explore.py can keep using split(directors, ',').
            people = spark.read.parquet(str(root / "clean" / "names")).select("nconst")
            for role in ("directors", "writers"):
                refs = df.select("tconst", F.posexplode(F.split(role, ",")).alias("pos", "nconst"))
                issues.append((name, f"unknown person in {role} removed", refs.join(people, "nconst", "left_anti").count()))
                kept = (refs.join(people, "nconst", "left_semi").groupBy("tconst")
                        .agg(F.array_join(F.transform(F.array_sort(F.collect_list(F.struct("pos", "nconst"))),
                                                      lambda x: x["nconst"]), ",").alias(role)))
                df = df.drop(role).join(kept, "tconst", "left")
            df = df.select(*columns.split())
        if name == "names":
            issues.append((name, "death before birth", df.filter("deathYear < birthYear").count()))
            df = df.withColumn("deathYear", F.when(F.col("birthYear").isNull() | (F.col("deathYear") >= F.col("birthYear")), F.col("deathYear")))
        if name in {"akas", "crew", "episodes", "ratings", "principals"}:
            links = [("titleId" if name == "akas" else "tconst", "titles", "tconst")]
            if name == "episodes":
                links.append(("parentTconst", "titles", "tconst"))
            if name == "principals":
                links.append(("nconst", "names", "nconst"))
            for key, parent, pk in links:
                ids = spark.read.parquet(str(root / "clean" / parent)).select(F.col(pk).alias(key))
                issues.append((name, "missing parent " + key, df.join(ids, key, "left_anti").count()))
                df = df.join(ids, key, "left_semi")
        save(df, root / "clean" / name)
        raw_cache.unpersist()
        repeated.unpersist()
        tsv.unlink()
    audit = spark.createDataFrame(profile, "dataset string, field string, rows long, missing long, percent double")
    save(audit, root / "results/missing")
    save(audit.orderBy(F.desc("percent"), "dataset", "field").limit(5), root / "results/top_missing")
    save(spark.createDataFrame(issues, "dataset string, issue string, affected long"), root / "results/quality")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("--root", type=Path, default=Path("work"))
    cli.add_argument("--download", action="store_true")
    args = cli.parse_args()
    if args.download:
        download(args.root / "raw")
    prepare(session(), args.root)
