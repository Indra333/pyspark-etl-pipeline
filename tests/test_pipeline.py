"""Pipeline unit tests. Run with: pytest tests/"""
import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from quality import DataQualityError, run_quality_checks
from transform import build_daily_sales_fact, clean_customers, clean_orders


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("pipeline-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_order_dedupe_keeps_latest(spark):
    rows = [
        ("1", "1", "2024-08-02", "Books", "1", "1000"),
        ("1", "1", "2024-08-09", "Books", "2", "1000"),  # newer duplicate
    ]
    df = spark.createDataFrame(
        rows, ["order_id", "customer_id", "order_date", "product_category", "quantity", "unit_price_cents"]
    )
    result = clean_orders(df)
    assert result.count() == 1
    row = result.collect()[0]
    assert row["quantity"] == 2
    assert row["line_total_cents"] == 2000


def test_scd_type1_keeps_latest_customer(spark):
    rows = [
        ("1", "Ann", "Lee", "ANN@EXAMPLE.COM", "Austin", "TX", "2024-01-01"),
        ("1", "Ann", "Lee", "ann.lee@example.com", "Denver", "CO", "2024-06-01"),
    ]
    df = spark.createDataFrame(
        rows, ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"]
    )
    result = clean_customers(df)
    assert result.count() == 1
    row = result.collect()[0]
    assert row["city"] == "Denver"  # latest record wins
    assert row["email"] == "ann.lee@example.com"  # normalized to lowercase


def test_fact_revenue_math(spark):
    customers = spark.createDataFrame(
        [("1", "Ann", "Lee", "a@e.com", "Austin", "TX", "2024-01-01")],
        ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"],
    )
    orders = spark.createDataFrame(
        [
            ("1", "1", "2024-08-01", "Books", "2", "1500"),
            ("2", "1", "2024-08-01", "Books", "1", "2000"),
        ],
        ["order_id", "customer_id", "order_date", "product_category", "quantity", "unit_price_cents"],
    )
    fact = build_daily_sales_fact(clean_orders(orders), clean_customers(customers))
    assert fact.count() == 1
    row = fact.collect()[0]
    assert row["num_orders"] == 2
    assert row["revenue_dollars"] == 50.00  # (2*1500 + 1*2000) / 100


def test_quality_check_fails_on_null_keys(spark):
    customers = spark.createDataFrame(
        [("1", "Ann", "Lee", "a@e.com", "Austin", "TX", "2024-01-01")],
        ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"],
    )
    bad_orders = spark.createDataFrame(
        [(None, "1", "2024-08-01", "Books", "1", "1000", 1000)],
        ["order_id", "customer_id", "order_date", "product_category", "quantity", "unit_price_cents", "line_total_cents"],
    )
    dim = clean_customers(customers)
    fact = build_daily_sales_fact(bad_orders, dim)
    with pytest.raises(DataQualityError):
        run_quality_checks(dim, bad_orders, fact, raw_order_count=1)
