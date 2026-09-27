"""Writers: dimension as a single Parquet dataset, fact partitioned by date."""
from typing import List
from pyspark.sql import DataFrame


def write_dimension(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def write_partitioned_fact(df: DataFrame, path: str, partition_cols: List[str]) -> None:
    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(path)
