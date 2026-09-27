"""Transformations: cleaning, SCD Type 1 dimension, daily sales fact."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_customers(df: DataFrame) -> DataFrame:
    """SCD Type 1 customer dimension: normalize fields, keep the latest
    record per customer_id (later signup_date wins)."""
    cleaned = (
        df.withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("state", F.trim(F.col("state")))
        .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
    )
    window = Window.partitionBy("customer_id").orderBy(F.col("signup_date").desc())
    return (
        cleaned.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def clean_orders(df: DataFrame) -> DataFrame:
    """Cast types, compute line totals, drop null keys, keep the latest
    version of each order_id."""
    cleaned = (
        df.withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        .withColumn("product_category", F.trim(F.col("product_category")))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price_cents", F.col("unit_price_cents").cast("int"))
        .filter(F.col("order_id").isNotNull())
    )
    cleaned = cleaned.withColumn(
        "line_total_cents", F.col("quantity") * F.col("unit_price_cents")
    )
    window = Window.partitionBy("order_id").orderBy(F.col("order_date").desc())
    return (
        cleaned.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def build_daily_sales_fact(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """Daily sales fact: orders joined to the customer dimension,
    aggregated to (order_date, product_category)."""
    joined = orders_df.join(customers_df, on="customer_id", how="inner")
    return (
        joined.groupBy("order_date", "product_category")
        .agg(
            F.countDistinct("order_id").alias("num_orders"),
            F.round(F.sum("line_total_cents") / 100.0, 2).alias("revenue_dollars"),
        )
        .orderBy("order_date", "product_category")
    )
