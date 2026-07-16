# Data Engineering Resources

Curated SQL patterns, optimization techniques, and architecture references from 7+ years of production data engineering.

## SQL Patterns

### Window Functions
```sql
-- Running total per customer, reset per month
SELECT
    customer_id,
    transaction_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id, DATE_TRUNC('month', transaction_date)
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_monthly_total,

    -- Lag / Lead for day-over-day delta
    amount - LAG(amount, 1, 0) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
    ) AS day_over_day_delta,

    -- Dense rank within segment
    DENSE_RANK() OVER (
        PARTITION BY customer_segment
        ORDER BY amount DESC
    ) AS segment_rank
FROM transactions;
```

### Slowly Changing Dimension (Type 2)
```sql
-- Find customers whose segment changed
SELECT
    c.customer_id,
    c.customer_segment AS current_segment,
    h.customer_segment AS previous_segment,
    c.effective_start_date AS changed_on
FROM dim_customer c
JOIN dim_customer h
    ON c.customer_id = h.customer_id
    AND h.is_current = false
    AND c.is_current = true
    AND c.customer_segment <> h.customer_segment
ORDER BY c.effective_start_date DESC;
```

### Incremental Load Pattern
```sql
-- Safe incremental insert — no duplicates even if re-run
INSERT INTO silver.sales
SELECT s.*
FROM   staging.sales_raw s
WHERE  s.updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM silver.sales)
  AND  NOT EXISTS (
           SELECT 1 FROM silver.sales t WHERE t.sale_id = s.sale_id
       );
```

### Outlier Detection
```sql
-- Flag statistical outliers using IQR method
WITH stats AS (
    SELECT
        product_id,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY unit_price) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unit_price) AS q3
    FROM sales
    GROUP BY product_id
)
SELECT s.*, st.q3 + 1.5 * (st.q3 - st.q1) AS upper_fence,
       CASE WHEN s.unit_price > st.q3 + 1.5 * (st.q3 - st.q1) THEN 'outlier' ELSE 'normal' END AS price_flag
FROM sales s
JOIN stats st ON s.product_id = st.product_id;
```

## Architecture Decision Records

### ADR-001: Delta Lake over Parquet for Silver/Gold layers
**Decision**: Use Delta Lake format for Silver and Gold; raw Parquet for Bronze.
**Reason**: ACID transactions + MERGE support essential for SCD Type 2 and safe re-processing. Bronze is append-only so plain Parquet suffices.

### ADR-002: Watermark-based CDC over full reload
**Decision**: Incremental watermark CDC for all tables > 1M rows.
**Reason**: Full reloads of large tables breach the 4-hour SLA window. Watermark approach reduces load by 95% on average.

### ADR-003: Metadata-driven pipelines in ADF
**Decision**: One parameterized ADF pipeline per pattern, controlled by a SQL metadata table.
**Reason**: Adding a new source table requires only a DB row insert — no pipeline code change, no deployment needed.

## Useful Databricks Commands

```python
# Check Delta table history
spark.sql("DESCRIBE HISTORY delta.`/mnt/gold/fact_sales`").show(10, truncate=False)

# Vacuum old files (retain 7 days)
spark.sql("VACUUM delta.`/mnt/gold/fact_sales` RETAIN 168 HOURS")

# Optimize and Z-order
spark.sql("OPTIMIZE delta.`/mnt/gold/fact_sales` ZORDER BY (customer_id, transaction_date)")

# Show table details
spark.sql("DESCRIBE DETAIL delta.`/mnt/gold/fact_sales`").show(truncate=False)

# Restore to previous version
spark.sql("RESTORE delta.`/mnt/gold/fact_sales` TO VERSION AS OF 42")
```

## Tools & Stack

![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=flat&logo=delta&logoColor=white)
![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-742774?style=flat&logo=microsoft&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white)

---

<!-- LAST_TIP -->
**Latest tip (2026-07-16):** [PySpark] Use coalesce instead of repartition when reducing
