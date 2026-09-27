# PySpark ETL Pipeline

An end-to-end batch ETL pipeline built with PySpark: raw CSVs go in, a clean customer dimension and a partitioned daily-sales fact come out as Parquet, with data-quality gates in between.

## Architecture

```
data/customers.csv ─┐
                    ├─► extract (typed CSV reads)
data/orders.csv ────┘
                        │
                        ▼
              transform
   ┌─ clean_customers: normalize + SCD Type 1
   │   (latest record per customer_id wins)
   └─ clean_orders: cast types, compute line totals,
       drop null keys, dedupe order_id (latest wins)
                        │
                        ▼
              quality gates (fail the run on violation)
   ┌─ null checks on keys
   ├─ duplicate checks on keys
   ├─ schema validation
   └─ row-count reconciliation (raw >= cleaned >= fact)
                        │
                        ▼
              load
   ┌─ customer_dim → Parquet
   └─ daily_sales_fact → Parquet, partitioned by order_date
```

The pipeline is split into small modules (`src/extract.py`, `transform.py`, `load.py`, `quality.py`) so each stage is independently testable — see `tests/test_pipeline.py`.

## How to run

Requires Java 8+ and Python 3.9+.

```bash
pip install -r requirements.txt
python run_pipeline.py --input-dir data --output-dir output
pytest tests/
```

Sample run output:

```
extracted: 13 customer rows, 17 order rows
transformed: 12 dim rows, 16 fact rows
  [PASS] customer_dim: no nulls in ['customer_id'] (ok)
  [PASS] customer_dim: no duplicate keys ['customer_id'] (rows=12, distinct_keys=12)
  ...
loaded parquet to output/
```

Note the sample data intentionally contains a duplicate customer record and a duplicate order — the pipeline resolves both (SCD Type 1 / latest-wins dedupe) and the quality gates verify the result.

## Tests

`tests/test_pipeline.py` covers dedupe behavior, SCD Type 1 semantics, revenue math, and verifies that the quality gates actually fail the run when fed null keys.
