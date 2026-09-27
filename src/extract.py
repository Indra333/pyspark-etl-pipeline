"""Shared Spark session and CSV schemas for the pipeline."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def get_spark(app_name: str = "pyspark-etl-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# Read everything as strings first; transform.py owns all casting.
# This mirrors landing-zone practice where raw files can't be trusted.
CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("signup_date", StringType(), True),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price_cents", StringType(), True),
    ]
)


def read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return spark.read.schema(schema).option("header", "true").csv(path)
