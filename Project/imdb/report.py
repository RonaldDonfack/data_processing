"""Refresh measured tables in REPORT.md, preserving team-written text."""
import argparse
import csv
import gzip
import json
from pathlib import Path

from pyspark.sql import functions as F

from prepare import session

START = "<!-- measured-output:start -->"
END = "<!-- measured-output:end -->"
ORDER = dict(runtime_outliers=["genre", "tconst"], annual_top_three=["startYear", "position"],
             annual_comparison=["startYear"], top_missing=["-percent", "dataset", "field"],
             missing=["dataset","field"], directors=["nconst"],tv_extremes=["parentTconst"],
             repeat_collaborations=["director"])


def cell(value):
    if isinstance(value, float):
        value = round(value, 5)
    return str(value if value is not None else "—").replace("|", r"\|").replace("\n", " ")


def table(cols, rows):
    return ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"]*len(cols)) + " |",
            *["| " + " | ".join(map(cell, row)) + " |" for row in rows]]


def render(root, report):
    spark = session()
    lines = []
    datasets = sorted(p for p in (root/"results").glob("*") if (p/"_SUCCESS").exists())
    if not datasets:
        raise ValueError("No successful result datasets")
    exports = report.parent / "Project" / "results"
    exports.mkdir(parents=True, exist_ok=True)
    for p in datasets:
        df = spark.read.parquet(str(p))
        total = df.count()
        columns = ORDER.get(p.name,df.columns[:2])
        order = [F.desc(c[1:]) if c.startswith("-") else F.asc(c) for c in columns]
        limit = total if total <= 150 else 15
        archive = exports / (p.name + ".csv.gz")
        temp = archive.with_suffix(".tmp")
        with gzip.open(temp, "wt", encoding="utf-8", newline="") as data:
            writer = csv.writer(data)
            writer.writerow(df.columns)
            for row in df.toLocalIterator():
                writer.writerow(json.dumps(v, ensure_ascii=False) if isinstance(v, (list,dict)) else v for v in row)
        temp.replace(archive)
        lines.extend([f"### {p.name}", "", f"{total:,} rows. [Complete CSV](Project/results/{archive.name}).", ""])
        lines.extend(table(df.columns, df.orderBy(*order).limit(limit).collect()))
        lines.append("")
    progress = root / "live/progress.jsonl"
    if progress.exists():
        rows = [json.loads(line) for line in progress.read_text().splitlines()]
        summaries = {}
        for row in rows:
            batches = summaries.setdefault(row["runId"],{})
            batches[row["batchId"]] = row
        lines += ["### Streaming progress", ""]
        lines += table(["Run", "Batches", "Kafka input rows", "Rows dropped by watermark"], [
            (run,len(b),sum(p["numInputRows"] for p in b.values()),
             sum(op.get("numRowsDroppedByWatermark",0) for p in b.values() for op in p["stateOperators"]))
            for run,b in summaries.items()])
    original = report.read_text(encoding="utf-8")
    if original.count(START)!=1 or original.count(END)!=1:
        raise ValueError("Expected exactly one measured-output block")
    before, tail = original.split(START)
    _, after = tail.split(END)
    output = before + START + "\n\n" + "\n".join(lines) + "\n\n" + END + after
    tmp = report.with_suffix(".tmp")
    tmp.write_text(output, encoding="utf-8")
    tmp.replace(report)


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("--root", type=Path, default=Path("work"))
    cli.add_argument("--report", type=Path, default=Path("REPORT.md"))
    args = cli.parse_args()
    render(args.root,args.report)
