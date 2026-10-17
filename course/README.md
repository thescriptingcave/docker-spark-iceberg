# Hands-on Apache Iceberg with PySpark and PyIceberg

A step-by-step course that runs entirely on this repo's Docker stack (Spark, Iceberg REST catalog, RustFS object storage).
No prior Iceberg knowledge needed: basic Python and SQL are enough.

## Start

```sh
docker compose up -d
```

Open Jupyter at http://localhost:8888 and go to the **`course`** folder. Work through the notebooks in order and
run every cell. Each lesson ends with a **Your turn** exercise; the solution is hidden under *Show solution*.

When you are done: `docker compose down` (add `-v` to also delete the stored data).

## Lessons

| # | Notebook | Topic | Time |
|---|---|---|---|
| 01 | `01-welcome-and-stack-tour` | What Iceberg is, how the stack fits together | 15 min |
| 02 | `02-your-first-iceberg-table` | Create, insert, query with PySpark | 25 min |
| 03 | `03-under-the-hood` | Snapshots, manifests, data files, metadata tables | 25 min |
| 04 | `04-updates-deletes-merge` | `UPDATE`, `DELETE`, `MERGE INTO` | 25 min |
| 05 | `05-time-travel-and-rollback` | Query old versions, undo mistakes | 20 min |
| 06 | `06-schema-and-partition-evolution` | Change columns and partitioning safely | 25 min |
| 07 | `07-pyiceberg-reading` | Scan, filter, DuckDB and pandas with PyIceberg | 25 min |
| 08 | `08-pyiceberg-writing` | Create, append, upsert, evolve with PyIceberg | 30 min |
| 09 | `09-maintenance-and-capstone` | Compaction, snapshot expiry, mini pipeline | 40 min |

Total: about 4 hours.

## Notes

* Lessons are independent: each one recreates the tables it needs, so you can re-run or skip around.
  Lesson 09 finishes by dropping everything the course created.
* `helpers.py` holds small setup helpers (Spark session, sample data) so the notebooks can focus on Iceberg.
* Tables live in the `demo.course` namespace. Browse the raw files in the RustFS console at http://localhost:9001
  (`admin` / `password`) under the `warehouse` bucket.
* The REST catalog keeps its state in memory: **restarting the `rest` container forgets all tables**, even though
  the files remain in object storage. Re-run the lesson that creates them.
* The REST catalog image is pinned to the same Iceberg version as the Spark client (1.9.0). Newer fixture images
  reject Spark's metadata-table queries such as `orders.snapshots` with `400 Suspicious Path Character`.

## Quick reference

```python
# Spark
spark.sql("CREATE TABLE demo.course.t (id BIGINT, v STRING) USING iceberg")
spark.createDataFrame(rows, schema).writeTo("demo.course.t").append()
spark.sql("SELECT * FROM demo.course.t.snapshots")                 # metadata tables
spark.sql("SELECT * FROM demo.course.t VERSION AS OF <snapshot_id>")  # time travel
spark.sql("CALL demo.system.rollback_to_snapshot('course.t', <snapshot_id>)")

# PyIceberg
from pyiceberg.catalog import load_catalog
tbl = load_catalog("default").load_table("course.t")
tbl.scan(row_filter="id > 10", selected_fields=("id",)).to_pandas()
tbl.append(arrow_table); tbl.upsert(arrow_table, join_cols=["id"]); tbl.delete("id = 3")
```
