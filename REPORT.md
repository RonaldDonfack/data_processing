# IMDb project results · gr-03

## Preparation

### Methodological explanation

The project processes seven IMDb datasets covering titles, people, alternative titles, crew members, episodes, principal credits and ratings. These tables are connected through IMDb identifiers, principally `tconst` for titles and `nconst` for people.

The preparation pipeline downloads the compressed TSV files and records their SHA-256 checksums. These checksums identify the files used during processing and support verification when reproducing an analysis. Each archive is temporarily decompressed before Spark reads it, allowing the input to be split across processing tasks. Cleaned datasets are saved in Parquet format for subsequent analysis.

Missingness is measured before cleaning to preserve an accurate description of the source data. Empty strings, whitespace-only values and IMDb’s `\N` marker are treated as missing. For each column, the missing percentage is calculated as

$$
\text{Missing percentage} = \frac{\text{Number of missing values}}{\text{Total number of rows}} \times 100
$$

The five columns with the highest percentages are selected across all seven datasets.

Numerical fields are converted to appropriate types. Values that cannot be converted become null and are counted separately as quality issues. Records with missing primary keys are excluded. When several records share the same primary key, all records in that group are excluded because the implementation has no authoritative rule for selecting one.

Additional checks address inconsistent dates, invalid runtimes, invalid ratings and broken relationships. A death year preceding a birth year is set to null. A title’s end year preceding its start year and a non-positive runtime are also set to null. Ratings must be between 1 and 10 and have at least one vote. Dependent records are checked against their parent tables, and unknown person identifiers are removed from director and writer lists.

The approach preserves usable attributes while removing relationships that cannot be resolved. However, excluding duplicate-key groups can discard valid information alongside conflicting records. Alternatives include retaining flagged records for manual review or selecting a record using an explicit priority rule. Imputation is another possibility for some attributes, although imputing years, runtimes or ratings could influence the analytical results.

Missingness also requires contextual interpretation. For example, an absent death year can be expected for a living person. A high missing percentage therefore does not automatically establish poor data quality.


## Exploration

### Methodological explanation

The exploration stage uses Spark SQL to join the cleaned datasets, calculate aggregates and apply ranking functions. The analyses use explicit operational definitions for terms such as popularity, success and early career. These definitions make the queries reproducible, while also determining which records qualify.

#### Unusual movie runtimes

The runtime analysis selects titles classified as movies with a runtime of at least 40 minutes. Films with multiple genres contribute to each listed genre. Within each genre, the query calculates the mean runtime and population standard deviation.

A movie–genre pair is selected when

$$
|x-\mu_g| > 2\sigma_g,
$$

where $x$ is the film’s runtime and $\mu_g$ and $\sigma_g$ are its genre’s mean and standard deviation.

This rule adjusts the threshold to the runtime distribution of each genre. Its limitation is sensitivity to extreme values, which can increase both the mean and standard deviation. An interquartile-range rule or median absolute deviation would provide alternative measures with greater resistance to extreme observations. The two-standard-deviation rule is a descriptive threshold and does not establish statistical significance.

#### Annual genre popularity

For movies released between 1980 and 2025, popularity is measured using the sum of IMDb votes received by films in each genre. Genres are ranked within each release year, with alphabetical ordering resolving equal vote totals.

The analysis also compares the combined vote totals of the three leading genres with those of all remaining genres. Their share is calculated as

$$
\text{Top-three share} =\frac{\text{Votes assigned to the top three genres}}{\text{Votes assigned to all genres}}.
$$

Multi-genre films contribute their votes to multiple genre totals. Consequently, this share describes the distribution of votes assigned to genres. It cannot be interpreted as a share of unique viewers or unique film votes.

The votes are those available in the dataset snapshot, grouped by film release year. They do not measure votes cast during each historical year. Older films have also had more time to accumulate votes. Alternative popularity measures include film counts, average votes per film or external audience figures, each addressing a different aspect of popularity.

#### Directors with one highly rated film

Director identifiers are expanded from the crew dataset and joined to movie ratings. A director qualifies when exactly one rated movie has a rating of at least 8.0, at least two other rated movies have ratings below 8.0, and those other movies average at most 6.0.

Unrated films do not contribute to the rating calculations. The output identifies directors matching these numerical conditions, but the conditions alone cannot explain why their films received different ratings. A minimum vote threshold could improve rating stability, while excluding less widely rated films.

#### Television series and episode extremes

The episode analysis excludes season zero and requires at least three seasons containing ten or more episodes each. Episode ratings are then compared across the qualifying series.

A series is selected when a lowest-rated episode occurs in its last recorded season and a highest-rated episode occurs within its first two seasons. Equal ratings are permitted, so the best or worst rating may also occur elsewhere. Series with identical best and worst ratings are excluded.

The last season is the highest season number in the available data. For an ongoing series, this season may be incomplete. Episodes without ratings contribute to season-size counts but cannot determine rating extremes. Comparing season averages would provide a broader assessment of consistency, although it would address a different question from identifying individual best and worst episodes.

#### Actors successful across languages

Actor and actress credits are joined to movies, ratings and original-language statements retrieved from Wikidata. A successful film is defined as having a rating of at least 7.0 and at least 1,000 votes.

The query selects performers with at least two successful films and at least two distinct associated languages. A separate output contains those associated with three or more languages. Multilingual films can contribute several languages.

The vote threshold reduces reliance on ratings based on very small audiences, but it can exclude successful films with limited IMDb participation. Missing Wikidata language statements also reduce coverage. These results describe the languages associated with credited films and do not establish which language a performer personally speaks in a particular scene.

#### Early-career rating peaks

The career analysis requires at least fifteen credited movies with known release years and a recorded career spanning twenty years or more. The early-career period is defined as the first third of the interval between the earliest and latest recorded film years.

A performer qualifies when both the highest rating and mean rating in the early period exceed their corresponding values in the later period. Using both comparisons reduces dependence on a single highly rated early film.

Alternative definitions include the first five films or the first ten career years. Each would produce a different grouping. The current approach also depends on the completeness of IMDb principal credits and uses film ratings as a proxy for career success.

#### Professionals holding multiple roles

Director, writer and actor credits are combined to identify professionals with at least two distinct roles across movies. Their films are divided according to whether they hold one or several of these roles on the same project.

The analysis reports film counts and average ratings for each situation. A paired summary includes professionals with available ratings in both groups and calculates their individual rating differences before aggregating them.

Comparing each person with themselves reduces differences arising from the composition of separate groups. However, budgets, genres, career stage and project selection can still influence the observed differences. The analysis therefore supports descriptive comparisons rather than causal conclusions.

## Enrichment

### Methodological explanation

The enrichment pipeline selects feature-length movies released between 1980 and 2025 and ranks them within each year and genre. The current implementation requires at least 1,000 votes and orders eligible films by descending average rating, descending vote count and IMDb identifier. It retains up to ten films per group.

The vote threshold limits the influence of exceptionally high ratings based on a small number of votes. It also narrows the eligible population and can leave some year–genre groups with fewer than ten films. This additional restriction must be disclosed when assessing coverage against the assignment.

For each selected film, the pipeline chooses up to three distinct principals in credit order. Selecting three principal records does not ensure that three biographies are available, so retrieval coverage must be checked separately.

Wikidata’s IMDb identifier property connects films and people to their corresponding entities. Linked English Wikipedia pages supply source text. Film synopses are extracted from sections such as “Plot” or “Synopsis,” while biographical information is extracted from the introductory section of a person’s article.

Collection takes place through HTTP requests. Schema enforcement, section extraction, text cleaning and joins are performed in Spark. The pipeline extracts existing source material without generating replacement synopses or biographies.

Source revision URLs are retained so that extracted text can be traced to a specific article version. Missing pages, unavailable sections and empty extracts receive explicit statuses. A SQLite cache stores lookup and revision responses, allowing interrupted collection to resume without repeating successful requests.

Requests are spaced apart, and the collector implements retries for selected temporary failures, rate-limit responses and server lag. Attribution and applicable Wikipedia licensing must accompany redistributed extracts.

Using one source simplifies provenance and collection logic. The trade-off is incomplete coverage, especially for films or principals without suitable English Wikipedia pages. Additional language editions or open-license databases could increase coverage, at the cost of more complex matching and licensing requirements.



## Streaming

### Methodological explanation

The streaming pipeline collects live Wikipedia change events, publishes them to Kafka and processes them with PySpark Structured Streaming.

The producer accepts edits and new pages in Wikipedia’s main namespace across language editions. This broad intake increases opportunities to capture relevant changes during a limited observation window.

Spark compares event page titles against a static lookup constructed from IMDb movie titles, television-series titles and people’s names. Labels are normalized by replacing underscores, trimming spaces and converting characters to lowercase. Selected disambiguation suffixes are removed from event titles. The lookup retains multiword labels of at least five characters that map to one IMDb identifier.

These rules reduce some ambiguous matches, but a matching page title still represents a candidate association. False matches can remain, and single-word names, translated titles or alternative spellings can be missed. Identifier-based matching could improve precision but would require an additional mapping source.

The consumer uses the event’s timestamp and applies a ten-minute watermark with event-ID deduplication. The watermark allows the system to bound retained state while tolerating delayed events within its configured event-time horizon. Events arriving beyond the supported horizon may be discarded, and deduplication does not provide unlimited historical protection.

A longer watermark would increase tolerance for delays while retaining more state. A shorter watermark would reduce state retention while increasing exposure to late arrivals. The appropriate setting requires observation of actual event delays.

Matched events are written to Parquet with event identifiers, timestamps, Wikipedia editions, page titles, IMDb identifiers and source URLs. Checkpoints retain processing progress and streaming state for recovery. Progress logs record completed batches, input counts and watermark-related metrics.

**Results status.** The published report records two completed batches containing 143 Kafka input rows and zero rows dropped by the watermark. These observations establish limited processing activity. They do not document a complete 15–30-minute test, the number of relevant matches or recovery under connection interruption.

The current producer handles connection errors and timeouts, but recovery from prematurely terminated chunked responses remains an implementation gap. A completed live run and validation of its matching results are still required.

## Open question

### Question and methodological explanation

**Among directors with sufficient observations, how do IMDb ratings differ between films involving previously credited actors and films without an observed earlier collaboration?**

The question examines whether recurring professional relationships are associated with differences in audience ratings. It requires combining director credits, actor credits, movie release years and ratings.

The analysis constructs director–actor pairs for each movie and identifies the earliest recorded release year for each pair. A collaboration is classified as repeated when the current film’s release year is later than that first year.

A director’s film is classified as involving repeat collaboration if at least one credited actor previously worked with that director. The data are then reduced to one observation per director and film, preventing films with several returning actors from being counted multiple times.

Directors must have at least two rated films in each comparison group. For each eligible director, the query calculates

$$
\Delta_d=\overline{R}_{d,\text{repeat}}-\overline{R}_{d,\text{first}}.
$$

A positive difference indicates a higher average rating for that director’s films involving a previously credited actor. The summary reports the number of eligible directors and the mean and median differences.

A pooled comparison across all films would give greater influence to directors with larger filmographies. The implemented director-level comparison instead measures differences within each eligible director’s recorded work.

Several limitations affect interpretation. Principal credits provide incomplete cast coverage. Release years cannot establish the order of films released in the same year, so those films do not establish a strictly earlier collaboration. Genre, budget, sequels, career stage and project selection may also influence ratings. Any observed association would therefore require further analysis before being attributed to the effect of repeated collaboration.


## Measured outputs

The following measurements are available in the current repository. The published outputs predate some changes to preparation and enrichment, so a final execution against the current code is needed to establish a consistent submission.

| Output | Published size | Description |
| --- | --- | --- |
| Annual genre rankings | 138 rows | Three genres for each year from 1980 to 2025 |
| Annual genre comparison | 46 rows | Top-three vote totals compared with remaining genres |
| Runtime outliers | 13,661 rows | Qualifying movie–genre pairs |
| Streaming progress | 2 batches | 143 Kafka input rows and zero watermark drops |

### annual_comparison

46 rows. [Complete CSV](Project/results/annual_comparison.csv.gz).

| startYear | top_votes | other_votes | top_share |
| --- | --- | --- | --- |
| 1980 | 7726000 | 9729690 | 0.44261 |
| 1981 | 5243612 | 6040354 | 0.4647 |
| 1982 | 7411880 | 9541672 | 0.43719 |
| 1983 | 6938935 | 8193783 | 0.45854 |
| 1984 | 9633612 | 12190308 | 0.44142 |
| 1985 | 11065037 | 10407515 | 0.51531 |
| 1986 | 9761898 | 11362295 | 0.46212 |
| 1987 | 10195501 | 13723527 | 0.42625 |
| 1988 | 11623864 | 13192883 | 0.46839 |
| 1989 | 11930293 | 12198384 | 0.49444 |
| 1990 | 13942450 | 16648643 | 0.45577 |
| 1991 | 12469693 | 15765195 | 0.44164 |
| 1992 | 13794066 | 15998350 | 0.46301 |
| 1993 | 16562952 | 20443879 | 0.44756 |
| 1994 | 27944593 | 20026143 | 0.58253 |
| 1995 | 26389408 | 27513996 | 0.48957 |
| 1996 | 17111061 | 20995158 | 0.44904 |
| 1997 | 25023842 | 31755502 | 0.44072 |
| 1998 | 27888337 | 26288237 | 0.51477 |
| 1999 | 30952664 | 39973446 | 0.43641 |
| 2000 | 28372636 | 34715520 | 0.44973 |
| 2001 | 33532269 | 45597393 | 0.42376 |
| 2002 | 34173875 | 46410064 | 0.42408 |
| 2003 | 35757514 | 42824916 | 0.45503 |
| 2004 | 38975500 | 55335816 | 0.41326 |
| 2005 | 32980479 | 48698048 | 0.40378 |
| 2006 | 41363580 | 55499849 | 0.42703 |
| 2007 | 39282781 | 63193209 | 0.38334 |
| 2008 | 42344219 | 62511999 | 0.40383 |
| 2009 | 43206455 | 61299420 | 0.41344 |
| 2010 | 44098541 | 66212898 | 0.39976 |
| 2011 | 51914078 | 60711468 | 0.46094 |
| 2012 | 47551515 | 63704783 | 0.42741 |
| 2013 | 54161094 | 76255164 | 0.41529 |
| 2014 | 63224064 | 71229770 | 0.47023 |
| 2015 | 50241531 | 57560463 | 0.46605 |
| 2016 | 50487561 | 64042891 | 0.44082 |
| 2017 | 48850910 | 59594331 | 0.45047 |
| 2018 | 45864122 | 57680488 | 0.44294 |
| 2019 | 46684319 | 60313038 | 0.43631 |
| 2020 | 23139604 | 28621849 | 0.44704 |
| 2021 | 37487221 | 43270328 | 0.46419 |
| 2022 | 39570415 | 45748093 | 0.4638 |
| 2023 | 34892647 | 43082773 | 0.44748 |
| 2024 | 27458504 | 36547158 | 0.429 |
| 2025 | 22285766 | 34385128 | 0.39325 |

### annual_top_three

138 rows. [Complete CSV](Project/results/annual_top_three.csv.gz).

| startYear | genre | films | votes | position |
| --- | --- | --- | --- | --- |
| 1980 | Drama | 1603 | 3168377 | 1 |
| 1980 | Adventure | 186 | 2483111 | 2 |
| 1980 | Horror | 167 | 2074512 | 3 |
| 1981 | Adventure | 197 | 2251716 | 1 |
| 1981 | Drama | 1569 | 1714732 | 2 |
| 1981 | Horror | 180 | 1277164 | 3 |
| 1982 | Drama | 1653 | 3077439 | 1 |
| 1982 | Sci-Fi | 55 | 2459195 | 2 |
| 1982 | Action | 450 | 1875246 | 3 |
| 1983 | Drama | 1695 | 2927303 | 1 |
| 1983 | Action | 471 | 2026291 | 2 |
| 1983 | Adventure | 185 | 1985341 | 3 |
| 1984 | Action | 422 | 3670468 | 1 |
| 1984 | Drama | 1692 | 3029954 | 2 |
| 1984 | Adventure | 185 | 2933190 | 3 |
| 1985 | Comedy | 795 | 4569978 | 1 |
| 1985 | Adventure | 183 | 3412634 | 2 |
| 1985 | Drama | 1693 | 3082425 | 3 |
| 1986 | Drama | 1804 | 4006072 | 1 |
| 1986 | Adventure | 193 | 2967083 | 2 |
| 1986 | Comedy | 778 | 2788743 | 3 |
| 1987 | Drama | 1899 | 4142084 | 1 |
| 1987 | Comedy | 791 | 3690167 | 2 |
| 1987 | Action | 592 | 2363250 | 3 |
| 1988 | Drama | 1788 | 4470900 | 1 |
| 1988 | Comedy | 773 | 4434660 | 2 |
| 1988 | Action | 660 | 2718304 | 3 |
| 1989 | Comedy | 868 | 5017123 | 1 |
| 1989 | Drama | 1810 | 3968215 | 2 |
| 1989 | Adventure | 196 | 2944955 | 3 |
| 1990 | Drama | 1736 | 6329956 | 1 |
| 1990 | Comedy | 826 | 4021404 | 2 |
| 1990 | Crime | 451 | 3591090 | 3 |
| 1991 | Drama | 1736 | 5593695 | 1 |
| 1991 | Crime | 360 | 3677349 | 2 |
| 1991 | Comedy | 805 | 3198649 | 3 |
| 1992 | Drama | 1634 | 6104068 | 1 |
| 1992 | Comedy | 807 | 4077645 | 2 |
| 1992 | Crime | 370 | 3612353 | 3 |
| 1993 | Drama | 1588 | 9008515 | 1 |
| 1993 | Comedy | 733 | 4505840 | 2 |
| 1993 | Crime | 383 | 3048597 | 3 |
| 1994 | Drama | 1592 | 15753309 | 1 |
| 1994 | Crime | 341 | 6508890 | 2 |
| 1994 | Comedy | 771 | 5682394 | 3 |
| 1995 | Drama | 1607 | 12533636 | 1 |
| 1995 | Crime | 329 | 7769550 | 2 |
| 1995 | Comedy | 758 | 6086222 | 3 |
| 1996 | Drama | 1587 | 7870061 | 1 |
| 1996 | Comedy | 819 | 4793597 | 2 |
| 1996 | Crime | 302 | 4447403 | 3 |
| 1997 | Drama | 1752 | 12640588 | 1 |
| 1997 | Action | 527 | 6772309 | 2 |
| 1997 | Comedy | 901 | 5610945 | 3 |
| 1998 | Drama | 1755 | 11795760 | 1 |
| 1998 | Comedy | 953 | 9870818 | 2 |
| 1998 | Crime | 345 | 6221759 | 3 |
| 1999 | Drama | 1866 | 15535271 | 1 |
| 1999 | Comedy | 997 | 8230202 | 2 |
| 1999 | Crime | 351 | 7187191 | 3 |
| 2000 | Drama | 1959 | 13034015 | 1 |
| 2000 | Comedy | 1014 | 8344136 | 2 |
| 2000 | Adventure | 169 | 6994485 | 3 |
| 2001 | Drama | 2131 | 14751233 | 1 |
| 2001 | Comedy | 1104 | 9523331 | 2 |
| 2001 | Adventure | 184 | 9257705 | 3 |
| 2002 | Drama | 2232 | 17075017 | 1 |
| 2002 | Adventure | 192 | 8696816 | 2 |
| 2002 | Action | 596 | 8402042 | 3 |
| 2003 | Drama | 2304 | 13269314 | 1 |
| 2003 | Action | 570 | 11718002 | 2 |
| 2003 | Comedy | 1279 | 10770198 | 3 |
| 2004 | Drama | 2393 | 16489605 | 1 |
| 2004 | Action | 589 | 11527982 | 2 |
| 2004 | Comedy | 1291 | 10957913 | 3 |
| 2005 | Drama | 2763 | 13704777 | 1 |
| 2005 | Comedy | 1466 | 10208103 | 2 |
| 2005 | Action | 549 | 9067599 | 3 |
| 2006 | Drama | 2995 | 20904177 | 1 |
| 2006 | Comedy | 1593 | 11789542 | 2 |
| 2006 | Thriller | 511 | 8669861 | 3 |
| 2007 | Drama | 3227 | 18551242 | 1 |
| 2007 | Comedy | 1643 | 11729822 | 2 |
| 2007 | Action | 600 | 9001717 | 3 |
| 2008 | Drama | 3677 | 17590562 | 1 |
| 2008 | Action | 675 | 13415631 | 2 |
| 2008 | Comedy | 1866 | 11338026 | 3 |
| 2009 | Drama | 4368 | 17607271 | 1 |
| 2009 | Comedy | 2207 | 13476288 | 2 |
| 2009 | Action | 801 | 12122896 | 3 |
| 2010 | Drama | 4872 | 17572231 | 1 |
| 2010 | Action | 967 | 13342039 | 2 |
| 2010 | Adventure | 526 | 13184271 | 3 |
| 2011 | Drama | 5035 | 20763650 | 1 |
| 2011 | Action | 987 | 17157322 | 2 |
| 2011 | Comedy | 2499 | 13993106 | 3 |
| 2012 | Drama | 5388 | 20129432 | 1 |
| 2012 | Action | 993 | 14902148 | 2 |
| 2012 | Comedy | 2663 | 12519935 | 3 |
| 2013 | Drama | 5890 | 21999373 | 1 |
| 2013 | Action | 1136 | 17194473 | 2 |
| 2013 | Comedy | 2871 | 14967248 | 3 |
| 2014 | Drama | 6157 | 24530980 | 1 |
| 2014 | Action | 1192 | 20946668 | 2 |
| 2014 | Adventure | 847 | 17746416 | 3 |
| 2015 | Drama | 6299 | 20505625 | 1 |
| 2015 | Action | 1238 | 15568119 | 2 |
| 2015 | Adventure | 744 | 14167787 | 3 |
| 2016 | Drama | 6461 | 20902105 | 1 |
| 2016 | Action | 1353 | 17216719 | 2 |
| 2016 | Adventure | 766 | 12368737 | 3 |
| 2017 | Drama | 6607 | 20821187 | 1 |
| 2017 | Action | 1422 | 16717452 | 2 |
| 2017 | Comedy | 3112 | 11312271 | 3 |
| 2018 | Drama | 6744 | 17425913 | 1 |
| 2018 | Action | 1353 | 15085552 | 2 |
| 2018 | Adventure | 732 | 13352657 | 3 |
| 2019 | Drama | 6652 | 22039665 | 1 |
| 2019 | Action | 1370 | 12369684 | 2 |
| 2019 | Comedy | 3016 | 12274970 | 3 |
| 2020 | Drama | 5270 | 10486681 | 1 |
| 2020 | Comedy | 2341 | 6519854 | 2 |
| 2020 | Action | 1054 | 6133069 | 3 |
| 2021 | Drama | 6422 | 14409526 | 1 |
| 2021 | Action | 1383 | 12882524 | 2 |
| 2021 | Adventure | 655 | 10195171 | 3 |
| 2022 | Drama | 7158 | 15991756 | 1 |
| 2022 | Action | 1486 | 13362059 | 2 |
| 2022 | Comedy | 3342 | 10216600 | 3 |
| 2023 | Drama | 7161 | 14786541 | 1 |
| 2023 | Action | 1537 | 10590682 | 2 |
| 2023 | Comedy | 3400 | 9515424 | 3 |
| 2024 | Drama | 6946 | 11686214 | 1 |
| 2024 | Action | 1520 | 8664420 | 2 |
| 2024 | Comedy | 3317 | 7107870 | 3 |
| 2025 | Drama | 6563 | 8664985 | 1 |
| 2025 | Action | 1567 | 7701741 | 2 |
| 2025 | Comedy | 3105 | 5919040 | 3 |

### runtime_outliers

13,661 rows. [Complete CSV](Project/results/runtime_outliers.csv.gz).

| tconst | primaryTitle | genre | runtimeMinutes | mean_minutes | sd |
| --- | --- | --- | --- | --- | --- |
| tt0003897 | The Exploits of Elaine | Action | 220 | 101.19641 | 33.74548 |
| tt0004052 | The Hazards of Helen | Action | 1428 | 101.19641 | 33.74548 |
| tt0004272 | Lucille Love: The Girl of Mystery | Action | 300 | 101.19641 | 33.74548 |
| tt0004313 | The Master Key | Action | 310 | 101.19641 | 33.74548 |
| tt0004465 | The Perils of Pauline | Action | 199 | 101.19641 | 33.74548 |
| tt0006206 | Les Vampires | Action | 421 | 101.19641 | 33.74548 |
| tt0006339 | The Adventures of Peg o' the Ring | Action | 300 | 101.19641 | 33.74548 |
| tt0006409 | Beatrice Fairfax | Action | 300 | 101.19641 | 33.74548 |
| tt0006917 | Liberty | Action | 400 | 101.19641 | 33.74548 |
| tt0007598 | The Yellow Menace | Action | 320 | 101.19641 | 33.74548 |
| tt0007932 | The Fatal Ring | Action | 400 | 101.19641 | 33.74548 |
| tt0008353 | The Mystery Ship | Action | 360 | 101.19641 | 33.74548 |
| tt0008355 | The Mystery of the Double Cross | Action | 350 | 101.19641 | 33.74548 |
| tt0008411 | Patria | Action | 310 | 101.19641 | 33.74548 |
| tt0008476 | The Purple Mask | Action | 330 | 101.19641 | 33.74548 |

### Streaming progress

| Run | Batches | Kafka input rows | Rows dropped by watermark |
| --- | --- | --- | --- |
| 1d59d8b2-ba28-4d96-a3b1-700a3c1b809f | 2 | 143 | 0 |

<!-- measured-output:end -->
