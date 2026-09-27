"""Entry point: extract -> transform -> quality gates -> load."""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from extract import CUSTOMERS_SCHEMA, ORDERS_SCHEMA, get_spark, read_csv
from load import write_dimension, write_partitioned_fact
from quality import run_quality_checks
from transform import build_daily_sales_fact, clean_customers, clean_orders


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the batch ETL pipeline.")
    parser.add_argument("--input-dir", default="data", help="Directory with customers.csv and orders.csv")
    parser.add_argument("--output-dir", default="output", help="Directory for Parquet output")
    args = parser.parse_args()

    spark = get_spark()

    raw_customers = read_csv(spark, f"{args.input_dir}/customers.csv", CUSTOMERS_SCHEMA)
    raw_orders = read_csv(spark, f"{args.input_dir}/orders.csv", ORDERS_SCHEMA)
    raw_order_count = raw_orders.count()
    print(f"extracted: {raw_customers.count()} customer rows, {raw_order_count} order rows")

    customer_dim = clean_customers(raw_customers)
    cleaned_orders = clean_orders(raw_orders)
    daily_fact = build_daily_sales_fact(cleaned_orders, customer_dim)
    print(f"transformed: {customer_dim.count()} dim rows, {daily_fact.count()} fact rows")

    results = run_quality_checks(customer_dim, cleaned_orders, daily_fact, raw_order_count)
    for r in results:
        print(f"  [PASS] {r.name} ({r.detail})")

    write_dimension(customer_dim, f"{args.output_dir}/customer_dim")
    write_partitioned_fact(daily_fact, f"{args.output_dir}/daily_sales_fact", ["order_date"])
    print(f"loaded parquet to {args.output_dir}/")

    spark.stop()


if __name__ == "__main__":
    main()
