# Walmart (29 May 2025)

## 1. How would you design a Dataproc/Spark cluster for a 20GB file?

* **Considerations**: File format, job complexity, transformations, and memory requirements.
* **Suggested Cluster**:

  * Master node: 1 (n1-standard-2)
  * Worker nodes: 2-3 (n1-standard-4 or custom with 4 vCPUs, 15 GB RAM)
  * Autoscaling enabled
  * Configure dynamic allocation and GCS as the storage layer.

## 2. What table/file formats have you worked with?

* **Common formats**: Parquet, ORC, Avro, Delta Lake, JSON, CSV, BigQuery tables, Hive tables.

## 3. Advantage of Delta Format over Parquet

* ACID transactions
* Scalable MERGE, UPDATE, DELETE
* Time travel (versioning)
* Schema evolution and enforcement
* Z-Order indexing

## 4. How to replicate DeltaMerge behavior with Parquet target?

* Read both datasets as DataFrames
* Perform join+filter+union operations
* Write updated records using overwrite mode or partition overwrite
* Requires manual tracking of updated vs inserted rows

## 5. What is Z-order clustering?

* Technique to colocate related data in the same set of files
* Optimizes read performance for multidimensional queries (e.g., WHERE customer\_id=... AND product\_id=...)
* Available in Delta Lake

## 6. Static vs Dynamic Partition Pruning

* **Static**: Partition filter known at compile time (e.g., hardcoded date)
* **Dynamic**: Partition filter resolved at runtime based on join keys
* Dynamic is crucial for query performance on large partitioned datasets

## 7. Spark Optimization Techniques

* Partition tuning (coalesce, repartition)
* Predicate pushdown
* Broadcast joins
* Caching intermediate results
* Avoiding shuffles, using filter early
* Using DataFrame API over RDD
* Enabling Tungsten/WholeStageCodeGen

## 8. cache() vs persist() in Spark

* `cache()` = `persist(StorageLevel.MEMORY_AND_DISK)`
* Use `persist()` for different storage levels (e.g., disk-only)

## 9. git fetch vs git pull

* `git fetch`: Downloads latest commits but doesn’t merge
* `git pull`: Fetches + merges into current branch

## 10. Deduplicate rows with Y/N flag preference

```sql
ROW_NUMBER() OVER (PARTITION BY id ORDER BY flag = 'Y' DESC) = 1
```

* Ensures 'Y' is preferred over 'N' if duplicate exists

## 11. When not to use RANK() or DENSE\_RANK()

* When unique row is needed per group (ROW\_NUMBER returns unique row, others may tie)
* ROW\_NUMBER is preferable for deduplication

## 12. What does dropDuplicates() do?

* Removes duplicates across specified columns
* Keeps first occurrence

```python
df.dropDuplicates(["col1", "col2"])
```

## 13. Group by item and retain proper flag

* Use `ROW_NUMBER()` as in Q10 or use `groupBy` + `agg(max(flag))` if flag priority is known

## 14. show(truncate=False) vs default show()

* Default `show()` truncates column values after 20 characters
* `show(truncate=False)` displays full column content

## 15. Drop duplicates using ROW\_NUMBER()

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY key_cols ORDER BY updated_ts DESC) AS rn
  FROM table
) WHERE rn = 1
```

## 16. Get item with max sales per day

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY sales DESC) as rn
  FROM sales_data
) WHERE rn = 1
```

## 17. Convert daily dates to start-end ranges

* Use `LAG` or `LEAD` and compare with current row’s date +1
* Group contiguous dates under same group id and take `MIN(date)` and `MAX(date)`
