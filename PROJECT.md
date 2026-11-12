# IMDb batch analytics and live Wikipedia edits

Group gr-03 · ECE Big Data Processing · Fall 2026

| Contributor | GitHub | GitHub commit email |
| --- | --- | --- |
| Ronald Donfack | RonaldDonfack | 110396032+RonaldDonfack@users.noreply.github.com |
| Dan Waintraub | Dan-Waintraub | 243867541+Dan-Waintraub@users.noreply.github.com |

## Environment

Use Linux, macOS, or Ubuntu on Windows via WSL2. Use Python 3.11 or 3.12, Java 17 or 21 and Docker with Compose v2. Allow roughly 50 GB of free local disk for a full run; the required space varies by snapshot. Run commands in Bash from the repository root. The integration branch contains all task branches; after peer-reviewed integration the same commands apply to main.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r Project/imdb/requirements.txt
java -version
export SPARK_LOCAL_IP=127.0.0.1
export WIKI_CONTACT='https://github.com/RonaldDonfack/data_processing'
export SPARK_LOCAL_DIRS="$PWD/work/spark-tmp"
mkdir -p "$SPARK_LOCAL_DIRS"
spark=(spark-submit --master 'local[2]' --driver-memory 1g --conf spark.sql.shuffle.partitions=96 --conf spark.sql.files.maxPartitionBytes=48m --conf spark.sql.execution.sortBeforeRepartition=false)
```

## Verify the installation

```bash
"${spark[@]}" --conf spark.sql.shuffle.partitions=2 Project/imdb/check_fixture.py
```

The temporary fixture checks duplicate-free joins, orphan removal, death-year correction, runtime outliers, a three-language actor, episode extrema, enrichment targets and query execution. Its rows are excluded from the report.

## Prepare a snapshot and run batch queries

```bash
"${spark[@]}" Project/imdb/prepare.py --download
"${spark[@]}" Project/imdb/explore.py --stage core
"${spark[@]}" Project/imdb/open_question.py
```

Raw files and SHA-256 checksums go in `work/raw/`. Keep the seven files together for reruns. To select a fresh snapshot, use a new root directory with `--root` on every command. IMDb permits personal and noncommercial use under its dataset terms. [Dataset source and terms](https://developer.imdb.com/non-commercial-datasets/)

Clean Parquet tables are in `work/clean/`. Whole-number and rating fields are typed. Quality results count repeated key groups, orphan rows and inconsistent lifespans. The raw missingness audit precedes all exclusions. Each batch query writes a separate Parquet dataset under `work/results/`.

## Collect film synopses, biographies and original languages

```bash
"${spark[@]}" Project/imdb/enrich.py --stage languages
"${spark[@]}" Project/imdb/explore.py --stage languages
"${spark[@]}" Project/imdb/enrich.py --stage text
```

The collector uses Wikidata IMDb identifiers (P345) and original-language statements (P364). Language results retain Wikidata entity URIs. Successful films are rated at least 7.0 with at least 1,000 votes. A multilingual film can contribute more than one original language.

Enrichment ranks feature-length movies (at least 40 minutes) by IMDb rating, vote count, then IMDb ID within each year and genre. Up to three distinct principals are chosen by credit order. Results are in `work/enriched/films`, `people` and `languages`. Inspect `results/coverage_*` and `results/insufficient_principals` for gaps. Retrieval is serial and rate limited. Keep `work/wikimedia-cache.sqlite` when restarting an interrupted request. Run one collector against a cache at a time. Article text comes from English Wikipedia contributors; each record links to its revision or lookup source. Preserve attribution and licensing when reusing text. [Wikimedia terms](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use)

## Capture and process a live test

Build the static lookup before starting the observation clock. Reuse the course Kafka service from Lab 4. Shut down another local Kafka broker if it occupies port 9092.

```bash
"${spark[@]}" Project/imdb/live.py index
docker compose -f Lab_4/compose.yaml up -d kafka
docker compose -f Lab_4/compose.yaml exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic imdb-wiki-v1 --partitions 1 --replication-factor 1
```

Start these two commands in separate terminals with the same environment. On first use, Spark downloads the Kafka connector and its dependencies from Maven. Use a fresh topic/checkpoint pair for an independent observation; keep both for recovery. The consumer starts at the earliest available offset on a fresh checkpoint. Start with an empty topic for a live-only measurement.

```bash
python Project/imdb/live.py publish --minutes 25
```

```bash
"${spark[@]}" --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  Project/imdb/live.py consume --minutes 25
```

The producer sends Wikipedia main-namespace edits and new pages from all Wikipedia languages. The consumer matches normalized page titles against unique multiword IMDb person and title names. These are candidate matches. Inspect their source URLs for homonyms. The consumer deduplicates event IDs within a ten-minute event-time watermark. Completed batches, watermarks and dropped-row counts are in `work/live/progress.jsonl`. Matches and restart offsets are in `work/live/matches` and `work/live/checkpoint`. Summaries are in `work/results/live_matches`. Stop the lab broker after collection with `docker compose -f Lab_4/compose.yaml stop kafka`.

## Render the results

```bash
"${spark[@]}" Project/imdb/report.py
```

The renderer replaces only the measured-output block in `REPORT.md`. Small results appear in full. Larger tables include an ordered preview. Each table links to a complete compressed CSV in `Project/results/`. To inspect any full result interactively, start `pyspark` and use `spark.read.parquet("work/results/directors").show(100, truncate=False)`. Work files, source dumps, caches and checkpoints belong outside version control.

## Development and submission

ChatGPT (OpenAI Codex) assisted with reading the brief, implementing the Spark/Kafka and Wikimedia code, constructing the fixture, running checks, preparing operations documentation, rendering measured tables and opening task PRs. The group authors the report explanations, alternatives, interpretations and takeaways. Group members should record their own contributions and peer-review findings. Each task requires a group member's approval before merge.

When all results and student-written sections have been reviewed, the course asks the group to send its repository link and team information to joe@adaltas.com with the subject `ECE big data processing Gr-03 git repo link`. [Submission instructions](https://github.com/RonaldDonfack/ece-big-data-processing-fall-2026/blob/main/README.md)
