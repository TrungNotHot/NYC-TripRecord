import os
from dagster import asset, AssetIn, Output, WeeklyPartitionsDefinition
import pandas as pd
from pyspark.sql import DataFrame
from ..resources.spark_io_manager import get_spark_session

WEEKLY = WeeklyPartitionsDefinition(start_date="2023-01-01")

@asset(
    name="silver_green_record",
    description="pick up datetime and Location in green taxi trips",
    ins={
        "bronze_green_record": AssetIn(
            key_prefix=["bronze", "trip_record"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
)
def silver_green_pickup(context, bronze_green_record: pd.DataFrame) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(silver_green_pickup) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {bronze_green_record.shape}"
        )

        spark_df = spark.createDataFrame(bronze_green_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")
        # transform
        spark_df.unpersist()

        return Output(
            spark_df,
            metadata={
                "table": "silver_green_pickup",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )