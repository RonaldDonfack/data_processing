"""Publish Wikimedia SSE to Kafka or consume the topic with Structured Streaming."""
import argparse
import json
import os
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from prepare import save, session

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
TOPIC = "imdb-wiki-v1"


def publish(args):
    import requests
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": args.broker, "enable.idempotence": True, "delivery.timeout.ms": 90000})
    errors = []
    last_id = None
    until = time.monotonic() + args.minutes * 60
    headers = {"User-Agent": "ECE-IMDb-coursework/1.0 (" + os.environ["WIKI_CONTACT"] + ")", "Accept": "text/event-stream"}
    def delivered(error, _message):
        if error:
            errors.append(str(error))
    while time.monotonic() < until:
        if last_id:
            headers["Last-Event-ID"] = last_id
        try:
            with requests.get(URL, headers=headers, stream=True, timeout=(15, 45)) as response:
                response.raise_for_status()
                for line in response.iter_lines():
                    if time.monotonic() >= until:
                        break
                    if line.startswith(b"id:"):
                        last_id = line[3:].strip().decode()
                    if not line.startswith(b"data:"):
                        continue
                    event = json.loads(line[5:])
                    if not (event.get("server_name", "").endswith(".wikipedia.org") and
                            event.get("namespace") == 0 and event.get("type") in {"edit", "new"}):
                        continue
                    while True:
                        try:
                            producer.produce(TOPIC, key=event["meta"]["id"], value=line[5:], on_delivery=delivered)
                            break
                        except BufferError:
                            producer.poll(0.5)
                    producer.poll(0)
                    if errors:
                        raise RuntimeError(errors[0])
        except (requests.ConnectionError, requests.Timeout):
            time.sleep(3)
    if producer.flush(95) or errors:
        raise RuntimeError(f"Kafka delivery incomplete: {errors[:1]}")


def norm(col):
    return F.lower(F.trim(F.regexp_replace(col, "_", " ")))


def index(spark, root):
    titles = spark.read.parquet(str(root / "clean/titles")).where("titleType IN ('movie','tvSeries')")
    films = titles.select(F.explode(F.array("primaryTitle", "originalTitle")).alias("label"), F.col("tconst").alias("id"))
    people = spark.read.parquet(str(root / "clean/names")).select(F.col("primaryName").alias("label"), F.col("nconst").alias("id"))
    return (films.union(people).select(norm("label").alias("label"), "id").where("length(label)>=5 AND instr(label,' ')>0")
            .groupBy("label").agg(F.min("id").alias("imdb_id"), F.countDistinct("id").alias("matches"))
            .where("matches=1").drop("matches"))


def consume(args):
    spark = session()
    lookup = args.root / "wiki-index"
    if not (lookup / "_SUCCESS").exists():
        save(index(spark, args.root), lookup)
    source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", args.broker)
              .option("subscribe", TOPIC).option("startingOffsets", args.offsets).load())
    schema = "title string, server_name string, timestamp long, meta struct<id:string,uri:string>"
    events = (source.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
              .selectExpr("e.meta.id event_id","e.meta.uri source_url","e.server_name wiki","e.title title",
                          "timestamp_seconds(e.timestamp) event_time")
              .where("event_id IS NOT NULL AND event_time IS NOT NULL")
              .withWatermark("event_time", "10 minutes").dropDuplicatesWithinWatermark(["event_id"])
              .withColumn("label", norm(F.regexp_replace("title", r"\s*\((?:\d{4} )?(?:film|actor|actress|director|TV series)\)$", ""))))
    matched = events.join(spark.read.parquet(str(lookup)), "label").drop("label")
    live = args.root / "live"
    live.mkdir(parents=True, exist_ok=True)
    query = (matched.writeStream.format("parquet").outputMode("append").option("path", str(live / "matches"))
             .option("checkpointLocation", str(live / "checkpoint")).trigger(processingTime="10 seconds").start())
    # Persist only completed micro-batches. Query progress is available even for zero matches.
    seen = set()
    end = time.monotonic() + args.minutes * 60
    try:
        with (live / "progress.jsonl").open("a") as out:
            while query.isActive and time.monotonic() < end:
                query.awaitTermination(2)
                for p in query.recentProgress:
                    raw = json.loads(p.json)
                    key = (raw["runId"],raw["batchId"])
                    if key not in seen:
                        out.write(json.dumps(raw) + "\n")
                        out.flush()
                        seen.add(key)
    finally:
        query.stop()
    files = list((live / "matches").glob("*.parquet"))
    if files:
        df = spark.read.parquet(str(live / "matches"))
        save(df.groupBy("wiki").agg(F.count("*").alias("events"),F.countDistinct("imdb_id").alias("entities"),
             F.min("event_time").alias("first_event"),F.max("event_time").alias("last_event")), args.root / "results/live_matches")
    else:
        print("No committed match files for this run.", file=sys.stderr)


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("mode", choices=["publish","consume","index"])
    cli.add_argument("--broker", default="localhost:9092")
    cli.add_argument("--root", type=Path, default=Path("work"))
    cli.add_argument("--minutes", type=int, default=25)
    cli.add_argument("--offsets", choices=["earliest","latest"], default="earliest")
    args = cli.parse_args()
    if args.mode == "index":
        save(index(session(), args.root), args.root / "wiki-index")
    elif args.mode == "publish":
        publish(args)
    else:
        consume(args)
