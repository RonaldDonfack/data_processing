"""Deterministic Spark integration check using synthetic data in a temporary directory."""
import gzip
from pathlib import Path
from tempfile import TemporaryDirectory

from enrich import targets
from explore import QUERIES, views
from open_question import QUERY
from prepare import TABLES, prepare, session
from report import END, START, render


def fixture(root):
    raw = root / "raw"
    raw.mkdir()
    data = {name: [] for name in TABLES}
    data["names"] = [
        ("nm1","Director One",1950,None,None,None),("nm2","Early Actor",1960,None,None,None),
        ("nm3","Other Actor",1970,1955,None,None),("nm4","Third Person",1970,None,None,None)]
    for i in range(15):
        film = f"tt{i:03}"
        data["titles"].append((film,"movie",f"Movie {i}",f"Movie {i}",0,1980+i*2,None,250 if i==0 else 90,"Drama"))
        data["ratings"].append((film,9 if i==0 else 7 if i in (1,2) else 5,2000))
        data["crew"].append((film,"nm1","nm1"))
        data["principals"].extend([(film,1,"nm2","actor",None,None),(film,2,"nm1","director",None,None)])
    data["principals"].append(("tt9999",1,"nm2","actor",None,None))
    data["akas"].append(("tt000",1,"Film zéro","FR","fr",None,None,0))
    data["titles"].append(("ttSeries","tvSeries","A Series","A Series",0,1990,None,None,"Drama"))
    for season in range(1,4):
        for ep in range(1,11):
            t = f"ttS{season}E{ep}"
            data["titles"].append((t,"tvEpisode",t,t,0,1990+season,None,30,"Drama"))
            data["episodes"].append((t,"ttSeries",season,ep))
            data["ratings"].append((t,10-season,1000))
    for name,(stem,cols) in TABLES.items():
        with gzip.open(raw/(stem+".tsv.gz"),"wt",encoding="utf-8") as out:
            out.write(cols.replace(" ","\t")+"\n")
            for row in data[name]:
                out.write("\t".join(r"\N" if v is None else str(v) for v in row)+"\n")


if __name__ == "__main__":
    spark = session()
    spark.sparkContext.setLogLevel("ERROR")
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        fixture(root)
        prepare(spark,root)
        views(spark,root)
        assert spark.table("principals").count()==30
        assert spark.table("names").where("nconst='nm3'").first().deathYear is None
        spark.createDataFrame([("tt000","English"),("tt001","French"),("tt002","German")],
                              "tconst string,language string").createOrReplaceTempView("languages_source")
        for name,sql in QUERIES.items():
            rows = spark.sql(sql).collect()
            assert rows, name
            if name=="languages":
                assert rows[0].language_count==3
            if name=="runtime_outliers":
                assert [r.tconst for r in rows]==["tt000"]
        films, people = targets(spark)
        assert films.count()==15 and people.count()==30
        assert spark.sql(QUERY).count()==0
        # Two distinct first casts in year one, then both actors return.
        spark.createDataFrame([("nm1","a"),("nm1","b"),("nm1","c"),("nm1","d")],
                              "nconst string,tconst string").createOrReplaceTempView("directed")
        spark.createDataFrame([("nm2","a"),("nm3","b"),("nm2","c"),("nm3","d")],
                              "nconst string,tconst string").createOrReplaceTempView("cast_movies")
        spark.createDataFrame([("a",2000,5.0),("b",2000,6.0),("c",2001,7.0),("d",2001,8.0)],
                              "tconst string,startYear int,averageRating double").createOrReplaceTempView("movies")
        assert spark.sql(QUERY).first().difference==2.0
        report = root / "REPORT.md"
        report.write_text("Student text.\n"+START+"\n"+END)
        render(root,report)
        assert report.read_text().startswith("Student text.")
        with gzip.open(root/"Project/results/top_missing.csv.gz","rt") as data:
            assert len(data.readlines())==6
    print("Passed: raw cleaning, all exploration queries, ranking targets, open question.")
