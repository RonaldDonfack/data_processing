"""Download the seven IMDb tables, audit raw cells and produce typed Parquet."""
import argparse
import hashlib
import json
import os
import shutil
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
        df = (spark.read.option("header", True).option("sep", "\t").option("quote", "")
              .option("mode", "FAILFAST").csv(str(root / "raw" / (stem + ".tsv.gz"))))
        if df.columns != columns.split():
            raise ValueError(f"Unexpected header for {stem}: {df.columns}")
        df = df.select(*(F.when(~F.trim(F.col(c)).isin("", r"\N"), F.trim(F.col(c))).alias(c) for c in df.columns))
        df = df.repartition(int(spark.conf.get("spark.sql.shuffle.partitions"))).persist(StorageLevel.DISK_ONLY)
        raw_cache = df
        stats = df.agg(F.count("*").alias("_rows"), *(F.count(c).alias(c) for c in df.columns)).first()
        total = stats["_rows"]
        for c in df.columns:
            missing = total - stats[c]
            profile.append((stem, c, total, missing, 100 * missing / total if total else None))
        for c in NUMBERS.intersection(df.columns):
            dtype = "double" if c == "averageRating" else "int"
            df = df.withColumn(c, F.expr(f"try_cast({c} as {dtype})"))
        keys = KEYS.get(name, [df.columns[0]])
        df = df.dropna(subset=keys)
        repeated = df.groupBy(*keys).count().filter("count > 1")
        repeated = repeated.persist(StorageLevel.DISK_ONLY)
        issues.append((name, "conflicting or repeated keys excluded", repeated.count()))
        df = df.join(repeated.select(*keys), keys, "left_anti")
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
