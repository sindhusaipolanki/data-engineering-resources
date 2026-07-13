#!/usr/bin/env python3
"""
Daily data engineering tip generator.
Picks today's tip from the curated list and appends it to the monthly log.
"""
import json
import os
from datetime import datetime, timezone

TIPS = [
    {
        "category": "Delta Lake",
        "title": "Use ZORDER BY for frequently filtered columns",
        "tip": "Run `OPTIMIZE table ZORDER BY (customer_id, date)` on your most-queried columns. Z-ordering co-locates related data in the same files, cutting query scan time by 50–90% on large tables.",
        "code": "OPTIMIZE delta.`/mnt/gold/fact_sales`\nZORDER BY (customer_id, transaction_date);"
    },
    {
        "category": "PySpark",
        "title": "Filter before join, not after",
        "tip": "Always apply `.filter()` on DataFrames before joining them. Spark pushes filters down, but explicit pre-filtering reduces shuffle size and speeds up joins significantly.",
        "code": "# Good — filter first, then join\nactive = customers.filter(col('status') == 'active')\nresult = orders.join(active, 'customer_id')\n\n# Bad — filters after join\nresult = orders.join(customers, 'customer_id').filter(col('status') == 'active')"
    },
    {
        "category": "SQL",
        "title": "Use window functions instead of self-joins",
        "tip": "Self-joins to compute running totals or previous values are slow and hard to read. Replace them with window functions — they run in a single pass over the data.",
        "code": "-- Running total per customer\nSELECT customer_id, amount,\n  SUM(amount) OVER (\n    PARTITION BY customer_id\n    ORDER BY transaction_date\n    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n  ) AS running_total\nFROM transactions;"
    },
    {
        "category": "Azure Data Factory",
        "title": "Use watermark tables for incremental loads",
        "tip": "Never do full-table reloads for large sources. Store the last successful load timestamp in a control table and use it as a filter in your source query. This reduces data transfer by 95%+ on mature pipelines.",
        "code": "-- ADF source query\nSELECT * FROM dbo.Sales\nWHERE updated_at > '@{activity('GetWatermark').output.firstRow.last_ts}'\n  AND updated_at <= '@{utcNow()}'"
    },
    {
        "category": "Delta Lake",
        "title": "Time travel for safe backfills",
        "tip": "Before a risky backfill, note the current Delta version with `DESCRIBE HISTORY`. If the backfill goes wrong, restore with `RESTORE TABLE ... TO VERSION AS OF N` — no need for a backup restore.",
        "code": "-- Check current version before backfill\nDESCRIBE HISTORY delta.`/mnt/gold/fact_sales`;\n\n-- Restore if something goes wrong\nRESTORE TABLE delta.`/mnt/gold/fact_sales` TO VERSION AS OF 42;"
    },
    {
        "category": "PySpark",
        "title": "Avoid Python UDFs — use native Spark functions",
        "tip": "Python UDFs serialize data to Python, process row-by-row, and serialize back. Native Spark functions (`pyspark.sql.functions`) run in the JVM and can be 10–100x faster. Use `pandas_udf` when you truly need Python logic.",
        "code": "# Bad — Python UDF (slow)\n@udf(StringType())\ndef upper(s): return s.upper() if s else None\n\n# Good — native function (fast)\nfrom pyspark.sql.functions import upper\ndf.withColumn('name', upper(col('name')))"
    },
    {
        "category": "SQL",
        "title": "CTEs improve readability and performance",
        "tip": "Common Table Expressions (CTEs) make complex queries readable and allow the optimizer to reuse subquery results. In most modern engines, a CTE referenced once is inlined; referenced twice, it may be materialized.",
        "code": "WITH monthly_sales AS (\n  SELECT DATE_TRUNC('month', sale_date) AS month,\n         SUM(amount) AS total\n  FROM sales\n  GROUP BY 1\n),\nranked AS (\n  SELECT *, RANK() OVER (ORDER BY total DESC) AS rnk\n  FROM monthly_sales\n)\nSELECT * FROM ranked WHERE rnk <= 3;"
    },
    {
        "category": "Microsoft Fabric",
        "title": "Use Direct Lake mode for Power BI on OneLake",
        "tip": "Direct Lake mode reads Parquet/Delta files from OneLake directly — no import, no DirectQuery overhead. It gives import-speed queries on always-fresh data. Pin your Gold layer tables to Direct Lake semantic models.",
        "code": "-- Ensure Delta table is optimized for Direct Lake\nOPTIMIZE gold.fact_sales;\nVACUUM gold.fact_sales RETAIN 168 HOURS;"
    },
    {
        "category": "PySpark",
        "title": "Cache DataFrames used more than twice",
        "tip": "If you read the same DataFrame in 3+ actions (count, write, join), call `.cache()` after first creation. Spark recomputes lineage from scratch on every action otherwise. Unpersist when done to free memory.",
        "code": "silver_df = transform(bronze_df).cache()\n\nrow_count = silver_df.count()          # action 1 — reads from cache\nsilver_df.write.delta.save(path)       # action 2 — reads from cache\nsilver_df.join(dim_df, 'id').show()    # action 3 — reads from cache\n\nsilver_df.unpersist()"
    },
    {
        "category": "Delta Lake",
        "title": "Set retention carefully before VACUUM",
        "tip": "Default Delta retention is 7 days (168 hours). If you have long-running jobs or streaming readers, increase retention before vacuuming to avoid breaking active readers mid-query.",
        "code": "-- Check active streams before vacuuming\n-- Increase retention if needed\nALTER TABLE delta.`/mnt/silver/events`\nSET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 14 days');\n\nVACUUM delta.`/mnt/silver/events` RETAIN 336 HOURS;"
    },
    {
        "category": "SQL",
        "title": "Lateral joins replace correlated subqueries",
        "tip": "Correlated subqueries run once per row — they're O(n²). Replace them with LATERAL (or CROSS APPLY in SQL Server) to evaluate once and join the result set efficiently.",
        "code": "-- Correlated subquery (slow)\nSELECT c.customer_id,\n  (SELECT MAX(sale_date) FROM sales s WHERE s.customer_id = c.customer_id) AS last_purchase\nFROM customers c;\n\n-- LATERAL join (fast)\nSELECT c.customer_id, s.last_purchase\nFROM customers c\nLEFT JOIN LATERAL (\n  SELECT MAX(sale_date) AS last_purchase FROM sales WHERE customer_id = c.customer_id\n) s ON true;"
    },
    {
        "category": "Azure Data Factory",
        "title": "Use metadata-driven pipelines to scale",
        "tip": "Instead of one pipeline per source table, create one parameterized pipeline and drive it from a metadata control table. Adding a new source = inserting one row. No deployments, no code changes.",
        "code": "-- Control table\nCREATE TABLE control.pipeline_config (\n  source_table   NVARCHAR(200),\n  sink_folder    NVARCHAR(500),\n  watermark_col  NVARCHAR(100),\n  active         BIT DEFAULT 1\n);\n-- ADF: ForEach over this table → run parameterized pipeline"
    },
    {
        "category": "PySpark",
        "title": "Broadcast small dimension tables",
        "tip": "For dimensions under ~100MB, broadcast them to all executors to avoid a shuffle join. Spark may auto-broadcast based on `spark.sql.autoBroadcastJoinThreshold`, but explicit hints are more reliable.",
        "code": "from pyspark.sql.functions import broadcast\n\n# Explicit broadcast hint\nresult = fact_df.join(\n    broadcast(dim_product),\n    'product_id'\n)"
    },
    {
        "category": "Delta Lake",
        "title": "Schema enforcement protects your pipeline",
        "tip": "Delta Lake enforces schema by default — writes with unexpected columns fail loudly. Use `mergeSchema=true` only at Bronze where sources evolve. Silver and Gold should fail on schema mismatches so you catch upstream changes early.",
        "code": "# Bronze — allow schema evolution\nbronze_df.write.format('delta')\\\n  .option('mergeSchema', 'true')\\\n  .mode('append').save(bronze_path)\n\n# Silver — enforce strictly (default, no option needed)\nsilver_df.write.format('delta').mode('append').save(silver_path)"
    },
    {
        "category": "SQL",
        "title": "Use EXPLAIN ANALYZE before optimizing",
        "tip": "Never guess at performance bottlenecks — run EXPLAIN ANALYZE to see the actual execution plan and row estimates. Look for seq scans on large tables, bad row estimates, and nested loop joins on large datasets.",
        "code": "-- PostgreSQL / Synapse\nEXPLAIN ANALYZE\nSELECT c.customer_name, SUM(s.amount)\nFROM sales s\nJOIN customers c ON s.customer_id = c.customer_id\nWHERE s.sale_date >= '2024-01-01'\nGROUP BY c.customer_name;"
    },
    {
        "category": "Data Quality",
        "title": "Row count checks catch silent failures",
        "tip": "Always assert row count > 0 after every pipeline stage. Silent empty DataFrames that pass through to Gold corrupt downstream reports with no error message. A simple count check costs milliseconds.",
        "code": "def assert_not_empty(df, stage: str) -> None:\n    count = df.count()\n    if count == 0:\n        raise ValueError(f'{stage} produced 0 rows — check upstream source')\n    print(f'{stage}: {count:,} rows OK')"
    },
    {
        "category": "Microsoft Fabric",
        "title": "Partition your Lakehouse tables by date",
        "tip": "Always partition large Delta tables by date (year/month or date). Power BI and Spark both prune partitions automatically when date filters are applied, cutting scan size from 100% to just the requested partitions.",
        "code": "df.write.format('delta')\\\n  .partitionBy('year', 'month')\\\n  .mode('overwrite')\\\n  .save('Tables/gold/fact_sales')"
    },
    {
        "category": "PySpark",
        "title": "Use monotonically_increasing_id for surrogate keys",
        "tip": "Need a unique ID for every row in a large DataFrame? `monotonically_increasing_id()` generates unique 64-bit integers without a shuffle — unlike `row_number()` which requires sorting across partitions.",
        "code": "from pyspark.sql.functions import monotonically_increasing_id\n\ndf_with_id = df.withColumn('surrogate_key', monotonically_increasing_id())"
    },
    {
        "category": "SQL",
        "title": "Avoid SELECT * in production queries",
        "tip": "SELECT * reads all columns — even ones you don't need. On columnar formats (Parquet, Delta), this defeats column pruning and reads 3–10x more data than necessary. Always name your columns explicitly.",
        "code": "-- Bad — reads all columns from Parquet\nSELECT * FROM gold.fact_sales WHERE year = 2024;\n\n-- Good — only reads 4 columns from disk\nSELECT sale_id, customer_id, amount, transaction_date\nFROM gold.fact_sales\nWHERE year = 2024;"
    },
    {
        "category": "Azure Data Factory",
        "title": "Use variables for dynamic pipeline dates",
        "tip": "Hardcoding dates in ADF queries breaks on re-runs and backfills. Use pipeline parameters + system variables like `@utcNow()` and `@formatDateTime()` so pipelines work for any run date.",
        "code": "@formatDateTime(pipeline().parameters.run_date, 'yyyy-MM-dd')\n\n-- Dynamic source query\nSELECT * FROM dbo.Orders\nWHERE OrderDate = '@{formatDateTime(pipeline().parameters.run_date, 'yyyy-MM-dd')}'"
    },
    {
        "category": "Delta Lake",
        "title": "Use MERGE for idempotent upserts",
        "tip": "MERGE (Delta's upsert operation) is idempotent — safe to re-run if a job fails. It updates existing records and inserts new ones in a single atomic operation, replacing the delete-then-insert anti-pattern.",
        "code": "DeltaTable.forPath(spark, target_path)\\\n  .alias('t')\\\n  .merge(source_df.alias('s'), 't.id = s.id')\\\n  .whenMatchedUpdateAll()\\\n  .whenNotMatchedInsertAll()\\\n  .execute()"
    },
    {
        "category": "Data Quality",
        "title": "Track null rates per column at Silver layer",
        "tip": "Compute null rate for every column at Silver ingestion. Set a threshold (e.g., >5% = fail). Null spikes indicate upstream schema changes or source system issues — catch them before they reach Gold.",
        "code": "def null_rates(df) -> dict:\n    total = df.count()\n    return {\n        col: df.filter(F.col(col).isNull()).count() / total\n        for col in df.columns\n    }"
    },
    {
        "category": "PySpark",
        "title": "Repartition before writing to avoid small files",
        "tip": "Spark writes one file per partition. If your DataFrame has 200 partitions but only 1M rows, you get 200 tiny files — the small file problem. Coalesce or repartition before writing.",
        "code": "# Target ~128MB per file\n# For 10GB of data → 80 partitions\ndf.coalesce(80)\\\n  .write.format('delta')\\\n  .mode('append')\\\n  .save(output_path)"
    },
    {
        "category": "SQL",
        "title": "Index foreign key columns in fact tables",
        "tip": "Fact tables join to dimensions on foreign keys. Without indexes on those columns, every join does a full table scan. Add non-clustered indexes on all FK columns in your fact tables.",
        "code": "-- SQL Server / Azure SQL\nCREATE NONCLUSTERED INDEX IX_fact_sales_customer\n  ON dbo.fact_sales (customer_id)\n  INCLUDE (amount, transaction_date);\n\nCREATE NONCLUSTERED INDEX IX_fact_sales_product\n  ON dbo.fact_sales (product_id)\n  INCLUDE (amount, quantity);"
    },
    {
        "category": "Microsoft Fabric",
        "title": "Use Shortcuts to avoid data duplication",
        "tip": "OneLake Shortcuts let you reference data in Azure Data Lake, S3, or another Lakehouse without copying it. Use shortcuts to expose external data in your Fabric Lakehouse with zero egress cost.",
        "code": "# Create via Fabric UI: Lakehouse → New shortcut\n# Or via REST API:\n# POST /v1/workspaces/{id}/lakehouses/{id}/shortcuts\n# body: { name, type: 'AdlsGen2', target: { location, subpath } }"
    },
    {
        "category": "Data Quality",
        "title": "Use Great Expectations for automated data contracts",
        "tip": "Define data expectations as code — column types, value ranges, referential integrity. Run them in your pipeline as a quality gate. Failed expectations block the write and alert the team before bad data reaches consumers.",
        "code": "import great_expectations as gx\n\ncontext = gx.get_context()\nsuite = context.add_expectation_suite('silver.sales')\nsuite.add_expectation(\n    gx.expectations.ExpectColumnValuesToBeBetween(\n        column='amount', min_value=0, max_value=1_000_000\n    )\n)"
    },
    {
        "category": "PySpark",
        "title": "Use Adaptive Query Execution (AQE)",
        "tip": "AQE dynamically re-optimizes queries at runtime based on actual partition statistics. Enable it globally — it automatically handles skew joins, coalesces shuffle partitions, and switches join strategies.",
        "code": "spark = SparkSession.builder\\\n  .config('spark.sql.adaptive.enabled', 'true')\\\n  .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')\\\n  .config('spark.sql.adaptive.skewJoin.enabled', 'true')\\\n  .getOrCreate()"
    },
    {
        "category": "Delta Lake",
        "title": "Enable Change Data Feed for incremental reads",
        "tip": "Delta's Change Data Feed (CDF) tracks row-level inserts, updates, and deletes. Use it to propagate only changed records downstream instead of reprocessing entire tables.",
        "code": "-- Enable on existing table\nALTER TABLE silver.customers\nSET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');\n\n-- Read changes since version 10\nSELECT * FROM table_changes('silver.customers', 10)\nWHERE _change_type IN ('insert', 'update_postimage');"
    },
    {
        "category": "SQL",
        "title": "Partition elimination is your best optimization",
        "tip": "Filtering on a partition column eliminates entire file groups before reading. A query on a 10TB table with date partitioning might read only 27GB for a single month. Always design partition keys around your most common query patterns.",
        "code": "-- Table partitioned by year, month\n-- This query reads ONLY 2024/01 partition files\nSELECT SUM(amount)\nFROM fact_sales\nWHERE year = 2024 AND month = 1;"
    },
    {
        "category": "Azure Data Factory",
        "title": "Set retry policies on all Copy activities",
        "tip": "Network blips and transient source errors cause pipelines to fail unnecessarily. Set retry count to 3 with 30-second intervals on all Copy activities. This handles most transient failures without manual intervention.",
        "code": "// ADF Copy activity retry settings\n{\n  \"retry\": 3,\n  \"retryIntervalInSeconds\": 30,\n  \"timeout\": \"02:00:00\"\n}"
    },
    {
        "category": "Data Quality",
        "title": "Reconcile row counts between source and target",
        "tip": "After every load, compare row counts between source and destination. A mismatch signals a dropped partition, a failed partial write, or a filter bug. Log the delta and alert on >0.1% variance.",
        "code": "def reconcile(source_count: int, target_count: int, table: str) -> None:\n    delta = abs(source_count - target_count)\n    pct = delta / source_count if source_count > 0 else 1.0\n    if pct > 0.001:\n        raise ValueError(\n            f'{table}: source={source_count:,}, target={target_count:,}, '\n            f'delta={delta:,} ({pct:.2%}) — exceeds 0.1% threshold'\n        )"
    },
    {
        "category": "PySpark",
        "title": "Handle skewed joins with salting",
        "tip": "When one join key has millions of rows (e.g., a 'null' customer_id), that partition becomes a hotspot. Salt the key by adding a random suffix 0–N, replicate the other side N times, then join and remove the salt.",
        "code": "import random\nfrom pyspark.sql.functions import col, lit, concat, rand, floor\n\nN = 10  # salt factor\nsalted_fact = fact.withColumn('salt', (rand() * N).cast('int'))\\\n                   .withColumn('join_key', concat(col('customer_id'), lit('_'), col('salt')))\n\nexploded_dim = dim.crossJoin(\n    spark.range(N).withColumnRenamed('id', 'salt')\n).withColumn('join_key', concat(col('customer_id'), lit('_'), col('salt')))\n\nresult = salted_fact.join(exploded_dim, 'join_key').drop('salt', 'join_key')"
    },
    {
        "category": "Microsoft Fabric",
        "title": "Use Fabric Pipelines for orchestration over ADF",
        "tip": "If you're fully on Microsoft Fabric, use Fabric Data Pipelines (built on ADF engine) instead of standalone ADF. They run inside the Fabric workspace, share the same capacity, and integrate natively with OneLake and Notebooks.",
        "code": "# Fabric Pipeline activities:\n# - Copy data → ingest to OneLake Bronze\n# - Notebook  → run PySpark transform\n# - Script    → run SQL analytics\n# All orchestrated with the same ADF JSON structure"
    },
    {
        "category": "SQL",
        "title": "Use EXCEPT and INTERSECT for data reconciliation",
        "tip": "EXCEPT and INTERSECT are powerful for reconciliation — find rows in source but not target, or rows that match exactly. Much faster than a NOT EXISTS correlated subquery on large tables.",
        "code": "-- Rows in source that are missing from target\nSELECT id, customer_id, amount FROM staging.sales\nEXCEPT\nSELECT id, customer_id, amount FROM silver.sales;\n\n-- Rows that match exactly between source and target\nSELECT id, customer_id, amount FROM staging.sales\nINTERSECT\nSELECT id, customer_id, amount FROM silver.sales;"
    },
    {
        "category": "Delta Lake",
        "title": "Use liquid clustering instead of partition columns",
        "tip": "Liquid clustering (Delta 3.0+) replaces static partitioning. It automatically reorganizes data based on your cluster keys and adapts as query patterns evolve — no more partition strategy regret.",
        "code": "-- Create with liquid clustering\nCREATE TABLE gold.fact_sales\nCLUSTER BY (customer_id, transaction_date);\n\n-- Incrementally cluster existing data\nOPTIMIZE gold.fact_sales;"
    },
    {
        "category": "Data Quality",
        "title": "Test for referential integrity at Gold layer",
        "tip": "Before writing fact tables, verify all foreign keys resolve to dimension records. Orphaned fact rows (no matching customer/product) cause blank labels in reports and confuse business users.",
        "code": "orphaned = fact_df.join(\n    dim_customer.select('customer_id'),\n    'customer_id',\n    'left_anti'   # rows in fact with NO match in dim\n)\nif orphaned.count() > 0:\n    raise ValueError(f'{orphaned.count()} orphaned fact rows — missing customer dimension records')"
    },
    {
        "category": "PySpark",
        "title": "Use struct columns to group related fields",
        "tip": "Instead of many flat columns, group related fields into a struct. This improves schema readability, allows atomic updates, and reduces the number of top-level columns in wide Parquet files.",
        "code": "from pyspark.sql.functions import struct\n\ndf = df.withColumn('address', struct(\n    col('street').alias('street'),\n    col('city').alias('city'),\n    col('zip').alias('zip'),\n    col('country').alias('country')\n)).drop('street', 'city', 'zip', 'country')\n\n# Access: df.select('address.city')"
    },
    {
        "category": "Azure Data Factory",
        "title": "Use Mapping Data Flows for complex transforms",
        "tip": "ADF Mapping Data Flows let you build complex ETL logic visually without writing Spark code — joins, aggregations, pivots, surrogate keys. They compile to Spark and run on an auto-managed cluster.",
        "code": "// Mapping Data Flow steps:\n// Source → Filter → Derived Column → Aggregate → Sink\n// Each step is a visual node — no code required\n// Use 'Debug' mode to preview row counts at each step"
    },
    {
        "category": "SQL",
        "title": "DATE_TRUNC for consistent time-series grouping",
        "tip": "Use DATE_TRUNC to group by calendar periods consistently. It truncates a timestamp to the start of the period — month, week, quarter, year — preventing off-by-one errors from manual date math.",
        "code": "-- Sales by month\nSELECT\n  DATE_TRUNC('month', sale_date) AS month,\n  SUM(amount) AS total_sales,\n  COUNT(*) AS transaction_count\nFROM sales\nGROUP BY DATE_TRUNC('month', sale_date)\nORDER BY month;"
    },
    {
        "category": "Delta Lake",
        "title": "Shallow clone for testing without data copy",
        "tip": "Use SHALLOW CLONE to create a zero-copy clone of a Delta table for testing. It references the same underlying files — no data duplication — and you can write to the clone without affecting the original.",
        "code": "-- Create a test clone (no data copied)\nCREATE TABLE gold.fact_sales_test\nSHALLOW CLONE gold.fact_sales;\n\n-- Test your migration on the clone\nUPDATE gold.fact_sales_test SET amount = amount * 1.1 WHERE year = 2024;\n\n-- Drop when done\nDROP TABLE gold.fact_sales_test;"
    },
    {
        "category": "Data Quality",
        "title": "Profile data before building pipelines",
        "tip": "Always profile source data before writing ETL logic. Check: null rates, distinct counts, min/max, value distributions, and referential integrity. Surprises found in profiling are features; surprises in production are incidents.",
        "code": "# Quick profiling with PySpark\ndf.describe().show()                    # count, mean, stddev, min, max\ndf.select([F.countDistinct(c).alias(c) for c in df.columns]).show()\ndf.select([F.sum(F.col(c).isNull().cast('int')).alias(c) for c in df.columns]).show()"
    },
    {
        "category": "PySpark",
        "title": "Use foreachBatch for streaming-to-Delta upserts",
        "tip": "Structured Streaming's native Delta sink only supports append mode. To do upserts (MERGE) from a stream, use `foreachBatch` — it gives you a static DataFrame of each micro-batch to run arbitrary Delta operations on.",
        "code": "def upsert_batch(batch_df, batch_id):\n    batch_df = batch_df.dropDuplicates(['event_id'])\n    DeltaTable.forPath(spark, output_path)\\\n      .alias('t')\\\n      .merge(batch_df.alias('s'), 't.event_id = s.event_id')\\\n      .whenNotMatchedInsertAll()\\\n      .execute()\n\nstream.writeStream.foreachBatch(upsert_batch)\\\n  .option('checkpointLocation', checkpoint_path)\\\n  .start()"
    },
    {
        "category": "Microsoft Fabric",
        "title": "Use Eventhouse for real-time analytics",
        "tip": "For high-velocity event streams (IoT, clickstream, logs), use Fabric Eventhouse (KQL Database) instead of Delta Lake. KQL queries return results in milliseconds on billions of rows via columnar indexing.",
        "code": "// KQL — last 1 hour of events by type\nevents\n| where ingestion_time() > ago(1h)\n| summarize count() by event_type\n| order by count_ desc"
    },
    {
        "category": "SQL",
        "title": "Use GENERATE SERIES to fill date gaps",
        "tip": "Reports with gaps in time series confuse business users. Use GENERATE_SERIES (PostgreSQL/Synapse) or a calendar table to produce all dates, then LEFT JOIN your data — gaps show as 0 instead of missing rows.",
        "code": "-- Fill daily gaps for a 30-day report\nWITH calendar AS (\n  SELECT generate_series(\n    DATE_TRUNC('day', NOW()) - INTERVAL '30 days',\n    DATE_TRUNC('day', NOW()),\n    INTERVAL '1 day'\n  )::date AS report_date\n)\nSELECT c.report_date, COALESCE(SUM(s.amount), 0) AS total\nFROM calendar c\nLEFT JOIN sales s ON s.sale_date = c.report_date\nGROUP BY c.report_date\nORDER BY c.report_date;"
    },
    {
        "category": "Delta Lake",
        "title": "Set auto-optimize properties on high-write tables",
        "tip": "For tables with frequent small writes (streaming or incremental loads), enable auto-optimize. It automatically compacts small files after every write, preventing the small-file problem without a separate OPTIMIZE job.",
        "code": "ALTER TABLE silver.events\nSET TBLPROPERTIES (\n  'delta.autoOptimize.optimizeWrite' = 'true',\n  'delta.autoOptimize.autoCompact'   = 'true'\n);"
    },
    {
        "category": "Data Quality",
        "title": "Use checksums to validate file transfers",
        "tip": "When moving large files between storage accounts or cloud regions, always validate with MD5/SHA256 checksums. A corrupted file that passes row count checks but fails checksum is a silent data corruption risk.",
        "code": "import hashlib\n\ndef file_checksum(path: str) -> str:\n    h = hashlib.md5()\n    with open(path, 'rb') as f:\n        for chunk in iter(lambda: f.read(8192), b''):\n            h.update(chunk)\n    return h.hexdigest()\n\n# Compare source and destination checksums\nassert file_checksum(source) == file_checksum(dest), 'Checksum mismatch!'"
    },
    {
        "category": "PySpark",
        "title": "Use coalesce instead of repartition when reducing",
        "tip": "When reducing partition count, `coalesce(n)` avoids a full shuffle by merging local partitions — much cheaper than `repartition(n)`. Use `repartition` only when you need even distribution or want to increase partitions.",
        "code": "# Reducing partitions — use coalesce (no shuffle)\ndf.coalesce(10).write.parquet(output_path)\n\n# Increasing partitions or redistributing — use repartition (full shuffle)\ndf.repartition(200, 'customer_id').write.parquet(output_path)"
    },
    {
        "category": "Azure Data Factory",
        "title": "Use Dataflow debug sessions to iterate fast",
        "tip": "Turn on Data Flow Debug in ADF before building complex Mapping Data Flows. Debug mode starts a live Spark cluster that stays warm for your session — preview transformations instantly without deploying.",
        "code": "// In ADF Studio:\n// 1. Toggle 'Data Flow Debug' to ON (top of canvas)\n// 2. Wait ~2 min for cluster to start\n// 3. Click 'Data Preview' on any transformation node\n// 4. See live results with your actual data\n// Debug session auto-expires after 8 hours"
    },
    {
        "category": "SQL",
        "title": "Materialized views for expensive aggregations",
        "tip": "Repeatedly running the same GROUP BY aggregation on billions of rows is wasteful. Materialize it as a view or table and refresh on a schedule. Power BI and downstream queries read the pre-computed result.",
        "code": "-- Synapse Analytics / Dedicated SQL Pool\nCREATE MATERIALIZED VIEW gold.mv_monthly_sales\nWITH (DISTRIBUTION = HASH(customer_id))\nAS\nSELECT\n  DATE_TRUNC('month', sale_date) AS month,\n  customer_id,\n  SUM(amount) AS total_sales\nFROM silver.sales\nGROUP BY DATE_TRUNC('month', sale_date), customer_id;"
    },
    {
        "category": "Delta Lake",
        "title": "Use table properties to document your tables",
        "tip": "Add table properties as inline documentation — owner, SLA, source system, data classification. This metadata lives with the table in Delta logs and is queryable via DESCRIBE DETAIL.",
        "code": "ALTER TABLE gold.fact_sales\nSET TBLPROPERTIES (\n  'owner'               = 'data-engineering',\n  'sla'                 = 'refresh-by-6am-cst',\n  'source_system'       = 'erp,salesforce',\n  'data_classification' = 'internal',\n  'contact'             = 'de-team@company.com'\n);\n\n-- View them\nDESCRIBE DETAIL gold.fact_sales;"
    },
]


def get_tip_for_today() -> dict:
    today = datetime.now(timezone.utc)
    day_of_year = today.timetuple().tm_yday
    index = (day_of_year - 1) % len(TIPS)
    return TIPS[index]


def format_tip(tip: dict, date_str: str) -> str:
    category = tip["category"]
    title = tip["title"]
    tip_text = tip["tip"]
    code = tip.get("code", "")

    lines = [f"## {date_str} — {category}: {title}", "", tip_text, ""]
    if code:
        lang = "sql" if any(kw in code for kw in ["SELECT", "CREATE", "ALTER", "INSERT", "UPDATE", "--"]) else "python"
        lines += [f"```{lang}", code, "```", ""]
    lines.append("---")
    return "\n".join(lines)


def main():
    today = datetime.now(timezone.utc)
    date_str = today.strftime("%Y-%m-%d")
    month_str = today.strftime("%Y-%m")

    tip = get_tip_for_today()
    formatted = format_tip(tip, date_str)

    tips_dir = os.path.join(os.path.dirname(__file__), "..", "daily-tips")
    os.makedirs(tips_dir, exist_ok=True)
    monthly_file = os.path.join(tips_dir, f"{month_str}.md")

    # Check if today's entry already exists (avoid duplicates on re-run)
    if os.path.exists(monthly_file):
        with open(monthly_file, "r") as f:
            if date_str in f.read():
                print(f"Tip for {date_str} already exists — skipping.")
                return

    # Create monthly file header if new
    if not os.path.exists(monthly_file):
        header = f"# Daily Data Engineering Tips — {today.strftime('%B %Y')}\n\n"
        with open(monthly_file, "w") as f:
            f.write(header)

    with open(monthly_file, "a") as f:
        f.write("\n" + formatted + "\n")

    print(f"Added tip for {date_str}: [{category}] {tip['title']}")

    # Update index in README
    update_readme(date_str, tip)


def update_readme(date_str: str, tip: dict):
    readme_path = os.path.join(os.path.dirname(__file__), "..", "README.md")
    if not os.path.exists(readme_path):
        return

    with open(readme_path, "r") as f:
        content = f.read()

    badge_line = f"![Last tip](https://img.shields.io/badge/Last%20tip-{date_str.replace('-', '--')}-1A9E8F)"
    marker = "<!-- LAST_TIP -->"

    new_section = f"{marker}\n**Latest tip ({date_str}):** [{tip['category']}] {tip['title']}\n"

    if marker in content:
        import re
        content = re.sub(rf"{re.escape(marker)}.*?(?=\n##|\Z)", new_section, content, flags=re.DOTALL)
    else:
        content += f"\n\n{new_section}"

    with open(readme_path, "w") as f:
        f.write(content)


if __name__ == "__main__":
    main()
