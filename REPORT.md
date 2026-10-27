# Assignment #3: Report

**Name:**
**Student ID:**
**Email:**

---

## Seed and commands

Seed used for `datagen.py`:

The commands you ran, in order:

```bash

```

---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):



Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:



The five counts:

| rows_total | bad_time | bad_fare | removed | rows_kept |
| ---------- | -------- | -------- | ------- | --------- |
|            |          |          |         |           |

Two or three bad rows, as they appear in `trips.csv`:

```

```

---

## Part 2a: trips and revenue per zone per day

First ten rows, and one sentence on what they show:

```

```

## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```

```

## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql

```

```

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



---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.



---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.


