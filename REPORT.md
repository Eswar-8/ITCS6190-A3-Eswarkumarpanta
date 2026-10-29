# Assignment #3: Report

**Name:** Eswar kumar panta

**Student ID:** 801505751

**Email:** epanta@charlotte.edu

---

## Seed and commands

Seed used for `datagen.py`: 801505751

The commands you ran, in order:

```bash
python3 datagen.py 801505751
docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output
docker compose down

```

---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):

I convert the text fare with col("fare").try_cast("double"). Any value that comes back null (empty, N/A, unknown, 12,40) breaks rule 2 and is flagged bad_fare. Rule 1 is the boolean dropoff_ts <= pickup_ts, flagged bad_time. Both flags are computed once on the raw data and all five counts come from one aggregation: bad_time and bad_fare are summed separately, removed is the sum of bad_time OR bad_fare (so a row breaking both counts once), and rows_kept is the rest. The clean rows get fare as DOUBLE, day = to_date(pickup_ts) and duration_min in minutes, and are written to Parquet and read back from there, so parts 2 and 3 do not redo the CSV parse and cleaning.

Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:

With a DOUBLE column the CSV reader (default PERMISSIVE mode) silently turns every empty or non-numeric fare into null at read time. The bad fares would then be indistinguishable from missing values, I could not count them separately from the time-rule violations, and I could not see what they were. Reading text and converting with try_cast keeps the raw value visible until I decide to drop the row, and it works under ANSI mode, where a plain cast("N/A" as double) raises an error.

The five counts:

| rows_total | bad_time | bad_fare | removed | rows_kept |
| ---------- | -------- | -------- | ------- | --------- |
|     100000 |  1441    |     1484 |    2925 |     97075 |

Two or three bad rows, as they appear in `trips.csv`:

```

```

---

## Part 2a: trips and revenue per zone per day

First ten rows, and one sentence on what they show:

```
day,zone_id,zone_name,trips,revenue
2026-09-01,1,Uptown Center,319,6318.78
2026-09-01,2,First Ward,228,4299.48
2026-09-01,3,Fourth Ward,213,4074.82
2026-09-01,4,South End,240,4883.06
2026-09-01,5,Dilworth,172,3414.4
2026-09-01,6,Plaza Midwood,129,2535.29
2026-09-01,7,NoDa,166,3350.72
2026-09-01,8,University City,219,4375.68
2026-09-01,9,Ballantyne,123,2442.47
2026-09-01,10,SouthPark,191,3570.26
```

## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```
driver_id,day,trips
D001,2026-09-19,17
D002,2026-09-26,23
D003,2026-09-16,20
D004,2026-09-07,19
D005,2026-09-10,23
D006,2026-09-16,24
D007,2026-09-17,25
D008,2026-09-11,20
D009,2026-09-09,23
D010,2026-09-06,19
```

## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql
spark.sql("""
        SELECT z.borough,
               COUNT(*)                    AS trips,
               ROUND(SUM(t.fare), 2)       AS revenue,
               ROUND(AVG(t.distance_km), 2)    AS avg_distance_km
        FROM trips t
        JOIN zones z ON t.pickup_zone = z.zone_id
        GROUP BY z.borough
        ORDER BY revenue DESC
    """)
```

```
borough,trips,revenue,avg_distance_km
Central,39224,773201.95,5.34
South,17464,345573.31,5.36
North,15157,297778.81,5.32
East,13082,261509.07,5.43
West,12148,240056.84,5.36
```

---

## The two plans

`explain()` output of part 2a (DataFrame API):

```
AdaptiveSparkPlan isFinalPlan=false
+- Sort [day#59 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(day#59 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=1543]
      +- HashAggregate(keys=[day#59, zone_id#9, zone_name#10], functions=[count(1), sum(fare#57)])
         +- Exchange hashpartitioning(day#59, zone_id#9, zone_name#10, 200), ENSURE_REQUIREMENTS, [plan_id=1540]
            +- HashAggregate(keys=[day#59, zone_id#9, zone_name#10], functions=[partial_count(1), partial_sum(fare#57)])
               +- Project [fare#57, day#59, zone_id#9, zone_name#10]
                  +- BroadcastHashJoin [pickup_zone#52], [zone_id#9], Inner, BuildRight, false, false
                     :- Filter isnotnull(pickup_zone#52)
                     :  +- FileScan parquet [pickup_zone#52,fare#57,day#59] Batched: true, DataFilters: [isnotnull(pickup_zone#52)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/output/clean_trips], PartitionFilters: [], PushedFilters: [IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,fare:double,day:date>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1535]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,zone_name#10] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,zone_name:string>
```

`explain()` output of part 3 (Spark SQL):

```
AdaptiveSparkPlan isFinalPlan=false
+- Sort [revenue#191 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(revenue#191 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=1593]
      +- HashAggregate(keys=[borough#11], functions=[count(1), sum(fare#57), avg(distance_km#56)])
         +- Exchange hashpartitioning(borough#11, 200), ENSURE_REQUIREMENTS, [plan_id=1590]
            +- HashAggregate(keys=[borough#11], functions=[partial_count(1), partial_sum(fare#57), partial_avg(distance_km#56)])
               +- Project [distance_km#56, fare#57, borough#11]
                  +- BroadcastHashJoin [pickup_zone#52], [zone_id#9], Inner, BuildRight, false, false
                     :- Filter isnotnull(pickup_zone#52)
                     :  +- FileScan parquet [pickup_zone#52,distance_km#56,fare#57] Batched: true, DataFilters: [isnotnull(pickup_zone#52)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/output/clean_trips], PartitionFilters: [], PushedFilters: [IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,distance_km:double,fare:double>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1585]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,borough#11] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,borough:string>
```

Your comparison: where the file scans are, which operator is the join and of which kind,
where the exchanges are and why, and what the two plans have in common although they were
written in different languages.

Scans. Each plan has two file scans, one per source: a Parquet FileScan of clean_trips (columnar, only the needed columns are read: pickup_zone, fare and day in 2a, pickup_zone, distance_km, fare in 3, with IsNotNull(pickup_zone) pushed down) and a CSV FileScan of the tiny zones.csv.

Join. A BroadcastHashJoin ... Inner, BuildRight. zones is 25 rows, far under the broadcast threshold, so Spark ships it to every executor (BroadcastExchange) and joins each trip in place, with no shuffle of the big table. It is an inner join on pickup_zone = zone_id.

Exchanges. Besides the broadcast, two shuffles: a hashpartitioning exchange on the group keys between the partial and final HashAggregate, so every group lands on one partition, and a rangepartitioning exchange for the final Sort/ORDER BY, which needs a global order.

In common. The two plans have the same shape, because the DataFrame API and SQL both compile to the same Catalyst logical plan and then to the same physical operators: scan, filter, broadcast join, partial aggregate, shuffle, final aggregate, range shuffle, sort. They differ only in the grouping keys (day, zone vs borough), the aggregates computed and the columns each scan reads.


---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.

The logic would stay as it is: read the fare as text and try_cast, flag the two rules, count in one pass, write clean data as Parquet and aggregate with the DataFrame API or SQL. The window function in part 2b also scales, since it partitions by driver. What I would change first is the input and the layout: replace the single trips.csv on each executor's local disk with a shared store (HDFS or object storage) and ingest in a splittable, columnar format partitioned by day, so later queries prune files and read only needed columns. Next I would size spark.sql.shuffle.partitions (the default 200 would be too few for 200M rows) and leave adaptive execution on to coalesce and handle skew. I would keep the small zones table as a broadcast join. I would cache the clean data only if several queries reused it.


---

## Problems and fixes

 no problems encountered
What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.


