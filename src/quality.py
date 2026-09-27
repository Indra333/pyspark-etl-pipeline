"""Data-quality gates. Every check returns (name, passed, detail);
run_quality_checks raises DataQualityError if anything fails."""
from dataclasses import dataclass
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    pass


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def check_no_nulls(df: DataFrame, columns: List[str], label: str) -> CheckResult:
    null_counts = {
        c: df.filter(F.col(c).isNull()).count() for c in columns
    }
    bad = {c: n for c, n in null_counts.items() if n > 0}
    return CheckResult(
        name=f"{label}: no nulls in {columns}",
        passed=not bad,
        detail="ok" if not bad else f"nulls found: {bad}",
    )


def check_no_duplicates(df: DataFrame, columns: List[str], label: str) -> CheckResult:
    total = df.count()
    distinct = df.dropDuplicates(columns).count()
    return CheckResult(
        name=f"{label}: no duplicate keys {columns}",
        passed=total == distinct,
        detail=f"rows={total}, distinct_keys={distinct}",
    )


def check_row_count_reconciliation(
    raw_orders: int, cleaned_orders: int, fact_rows: int
) -> CheckResult:
    # Cleaning may only remove rows (null keys, duplicates); the fact is an
    # aggregation so it can only have fewer-or-equal rows than cleaned orders.
    passed = raw_orders >= cleaned_orders >= fact_rows and cleaned_orders > 0
    return CheckResult(
        name="row-count reconciliation (raw >= cleaned >= fact)",
        passed=passed,
        detail=f"raw={raw_orders}, cleaned={cleaned_orders}, fact={fact_rows}",
    )


def check_expected_schema(df: DataFrame, expected: List[str], label: str) -> CheckResult:
    actual = set(df.columns)
    missing = [c for c in expected if c not in actual]
    return CheckResult(
        name=f"{label}: expected schema present",
        passed=not missing,
        detail="ok" if not missing else f"missing columns: {missing}",
    )


def run_quality_checks(
    customer_dim: DataFrame,
    cleaned_orders: DataFrame,
    daily_fact: DataFrame,
    raw_order_count: int,
) -> List[CheckResult]:
    results = [
        check_no_nulls(customer_dim, ["customer_id"], "customer_dim"),
        check_no_duplicates(customer_dim, ["customer_id"], "customer_dim"),
        check_expected_schema(
            customer_dim,
            ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"],
            "customer_dim",
        ),
        check_no_nulls(cleaned_orders, ["order_id", "customer_id"], "cleaned_orders"),
        check_no_duplicates(cleaned_orders, ["order_id"], "cleaned_orders"),
        check_no_nulls(daily_fact, ["order_date", "product_category"], "daily_sales_fact"),
        check_expected_schema(
            daily_fact,
            ["order_date", "product_category", "num_orders", "revenue_dollars"],
            "daily_sales_fact",
        ),
        check_row_count_reconciliation(
            raw_order_count, cleaned_orders.count(), daily_fact.count()
        ),
    ]
    failures = [r for r in results if not r.passed]
    if failures:
        raise DataQualityError(
            "Data quality checks failed:\n"
            + "\n".join(f"  - {r.name}: {r.detail}" for r in failures)
        )
    return results
