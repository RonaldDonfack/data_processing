"""Collect sourced text and original-language statements, then join in Spark."""
import argparse
import itertools
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlencode

import requests
from pyspark.sql import Window, functions as F

from explore import views
from prepare import save, session

WD = "https://query.wikidata.org/sparql"
WP = "https://en.wikipedia.org/w/api.php"
FACT_SCHEMA = "imdb_id string, entity string, article string, languages array<string>, lookup_url string"
TEXT_SCHEMA = "imdb_id string, text string, source_url string, status string"


class Wikimedia:
    def __init__(self, root):
        self.http = requests.Session()
        self.http.headers["User-Agent"] = "ECE-IMDb-coursework/1.0 (" + os.environ["WIKI_CONTACT"] + ")"
        self.db = sqlite3.connect(root / "wikimedia-cache.sqlite")
        self.db.execute("CREATE TABLE IF NOT EXISTS cache (k TEXT PRIMARY KEY, value TEXT)")
        self.last = 0.0

    def cached(self, key):
        row = self.db.execute("SELECT value FROM cache WHERE k=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO cache VALUES (?,?)", (key, json.dumps(value)))
        self.db.commit()

    def query(self, url, params):
        for attempt in range(6):
            time.sleep(max(0, self.last + 1.1 - time.monotonic()))
            self.last = time.monotonic()
            try:
                response = self.http.get(url, params=params, timeout=(15, 90))
                if response.status_code in {429, 500, 502, 503, 504}:
                    from email.utils import parsedate_to_datetime
                    retry = response.headers.get("Retry-After", "0")
                    delay = int(retry) if retry.isdigit() else parsedate_to_datetime(retry).timestamp() - time.time()
                    time.sleep(max(delay, 2**attempt))
                    continue
                response.raise_for_status()
                result = response.json()
                if result.get("error", {}).get("code") == "maxlag":
                    time.sleep(5 + 2**attempt)
                    continue
                if "error" in result:
                    raise RuntimeError(result["error"])
                return result
            except (requests.Timeout, requests.ConnectionError):
                if attempt == 5:
                    raise
                time.sleep(2**attempt)
        raise RuntimeError("Wikimedia temporarily unavailable; rerun to resume from cache")

    def facts(self, ids):
        missing = [i for i in ids if self.cached("id:" + i) is None]
        if missing:
            if any(not re.fullmatch(r"(tt|nm)\d+", i) for i in missing):
                raise ValueError("Invalid IMDb identifier")
            values = " ".join(json.dumps(i) for i in missing)
            query = f"""SELECT ?id ?item ?article ?lang WHERE {{ VALUES ?id {{ {values} }}
            ?item wdt:P345 ?id . OPTIONAL {{ ?item wdt:P364 ?lang }}
            OPTIONAL {{ ?article schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> }} }}"""
            source = WD + "?" + urlencode({"query": query})
            records = {i: dict(imdb_id=i, entity=None, article=None, languages=[], lookup_url=source) for i in missing}
            bindings = self.query(WD, dict(query=query, format="json"))["results"]["bindings"]
            for b in sorted(bindings, key=lambda row: row["item"]["value"]):
                rec = records[b["id"]["value"]]
                if rec["entity"] and rec["entity"] != b["item"]["value"]:
                    continue
                rec["entity"] = b["item"]["value"]
                if "article" in b:
                    rec["article"] = b["article"]["value"]
                if "lang" in b and b["lang"]["value"] not in rec["languages"]:
                    rec["languages"].append(b["lang"]["value"])
            for i, rec in records.items():
                self.put("id:" + i, rec)
        return [self.cached("id:" + i) for i in ids]

    def articles(self, facts):
        import mwparserfromhell
        from urllib.parse import unquote
        from collections import defaultdict
        by_title = defaultdict(list)
        for fact in facts:
            key = "text:" + fact["imdb_id"]
            if self.cached(key) is not None:
                continue
            rec = dict(imdb_id=fact["imdb_id"], text=None, source_url=fact["entity"] or fact["lookup_url"],
                       status="no_enwiki_page")
            if not fact["article"]:
                self.put(key, rec)
                continue
            title = unquote(fact["article"].split("/wiki/", 1)[1]).replace("_", " ")
            by_title[title].append(rec)
        titles = iter(by_title)
        while batch := list(itertools.islice(titles, 12)):
            params = dict(action="query", prop="revisions", titles="|".join(batch), rvprop="ids|content",
                          rvslots="main", redirects=1, format="json", formatversion=2, maxlag=5)
            result = self.query(WP, params)["query"]
            redirects = {v["from"]: v["to"] for group in ("normalized", "redirects") for v in result.get(group, [])}
            pages = {p["title"]: p for p in result["pages"]}
            for title in batch:
                target = title
                visited = set()
                while target in redirects and target not in visited:
                    visited.add(target)
                    target = redirects[target]
                page = pages.get(target, {})
                for rec in by_title[title]:
                    if page.get("revisions"):
                        rev = page["revisions"][0]
                        source = rev["slots"]["main"].get("content", "")
                        doc = mwparserfromhell.parse(source)
                        if rec["imdb_id"].startswith("nm"):
                            sections = doc.get_sections(include_headings=False, include_lead=True, flat=True)[:1]
                        else:
                            sections = doc.get_sections(matches=r"(?i)^\s*(plot|plot summary|plot synopsis|synopsis|premise)\s*$",
                                                        include_headings=False)[:1]
                        rec.update(source_url=f"https://en.wikipedia.org/w/index.php?oldid={rev['revid']}",
                                   status="section_unavailable")
                        if sections:
                            section = sections[0]
                            for tag in section.filter_tags(recursive=False):
                                if str(tag.tag).lower() in {"ref", "table", "gallery", "math"}:
                                    section.remove(tag)
                            value = section.strip_code(normalize=True, collapse=True, keep_template_params=False).strip()
                            rec.update(text=value or None, status="available" if value else "empty_section")
                    self.put("text:" + rec["imdb_id"], rec)
        return [self.cached("text:" + f["imdb_id"]) for f in facts]


def collect(ids, service, target, include_text):
    iterator = (r[0] for r in ids.distinct().toLocalIterator())
    temp = target.with_suffix(".tmp")
    with temp.open("w", encoding="utf-8") as out:
        while batch := list(itertools.islice(iterator, 35)):
            facts = service.facts(batch)
            for record in service.articles(facts) if include_text else facts:
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
    temp.replace(target)


def targets(spark):
    films = spark.sql("""SELECT * FROM (SELECT startYear,genre,tconst,primaryTitle,averageRating,numVotes,
        row_number() OVER(PARTITION BY startYear,genre ORDER BY averageRating DESC,numVotes DESC,tconst) position
        FROM film_genres WHERE startYear BETWEEN 1980 AND 2025 AND averageRating IS NOT NULL) WHERE position<=10""")
    credits = spark.table("principals").join(films.select("tconst").distinct(), "tconst")
    ordered = Window.partitionBy("tconst").orderBy("ordering", "nconst")
    people = (credits.groupBy("tconst", "nconst").agg(F.min("ordering").alias("ordering"))
              .withColumn("position", F.row_number().over(ordered)).where("position<=3"))
    return films, people


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("--root", type=Path, default=Path("work"))
    cli.add_argument("--stage", choices=["all", "languages", "text"], default="all")
    args = cli.parse_args()
    root = args.root
    spark = session()
    views(spark, root)
    api = Wikimedia(root)
    if args.stage in {"all", "languages"}:
        ids = spark.sql("SELECT tconst FROM movies WHERE averageRating>=7 AND numVotes>=1000")
        path = root / "language-facts.jsonl"
        collect(ids, api, path, False)
        facts = spark.read.schema(FACT_SCHEMA).json(str(path))
        save(facts.selectExpr("imdb_id AS tconst", "explode_outer(languages) AS language", "entity", "lookup_url"),
             root / "enriched/languages")
    if args.stage in {"all", "text"}:
        films, people = targets(spark)
        path = root / "wiki-text.jsonl"
        collect(films.select("tconst").union(people.select("nconst")), api, path, True)
        docs = spark.read.schema(TEXT_SCHEMA).json(str(path))
        save(films.join(docs.withColumnRenamed("imdb_id", "tconst"), "tconst"), root / "enriched/films")
        save(people.join(docs.withColumnRenamed("imdb_id", "nconst"), "nconst"), root / "enriched/people")
        coverage = films.select("tconst").distinct().join(people.groupBy("tconst").count(), "tconst", "left").fillna(0)
        save(coverage.where("count<3"), root / "results/insufficient_principals")
        for name in ("films", "people"):
            data = spark.read.parquet(str(root / "enriched" / name))
            save(data.groupBy("status").count(), root / "results" / ("coverage_" + name))
