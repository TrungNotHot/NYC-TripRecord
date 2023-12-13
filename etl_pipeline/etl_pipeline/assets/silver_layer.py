import os
from dagster import asset, AssetIn, Output, WeeklyPartitionsDefinition
import pandas as pd
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import col
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
def silver_green_record(
    context, bronze_green_record: pd.DataFrame
) -> Output[DataFrame]:
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
                "table": "silver_green_record",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    name="silver_fhv_pickup",
    description="pick up datetime and Location in fhv taxi trips",
    ins={
        "bronze_fhv_record": AssetIn(
            key_prefix=["bronze", "trip_record"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
)
def silver_fhv_pickup(context, bronze_fhv_record: pd.DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(silver_fhv_pickup) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {bronze_fhv_record.shape}"
        )

        spark_df = spark.createDataFrame(bronze_fhv_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")
        spark_df.createOrReplaceTempView("bronze_fhv_record")
        sql_stm = """
        SELECT 
            CONCAT("F", YEAR(pickup_datetime), LPAD(MONTH(pickup_datetime), 2, '0'), FLOOR(RAND() * 1000000)) AS PickUpID,
            pickup_datetime, 
            PUlocationID
        FROM bronze_fhv_record
        GROUP BY pickup_datetime, PUlocationID
        """
        spark_df = spark.sql(sql_stm)
        spark_df.unpersist()

        return Output(
            spark_df,
            metadata={
                "table": "silver_fhv_pickup",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    name="silver_fhv_dropoff",
    description="drop off datetime and Location in fhv taxi trips",
    ins={
        "bronze_fhv_record": AssetIn(
            key_prefix=["bronze", "trip_record"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
)
def silver_fhv_dropoff(context, bronze_fhv_record: pd.DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(silver_fhv_dropoff) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {bronze_fhv_record.shape}"
        )

        spark_df = spark.createDataFrame(bronze_fhv_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")
        spark_df.createOrReplaceTempView("bronze_fhv_record")
        sql_stm = """
        SELECT 
            CONCAT("F", YEAR(dropOff_datetime), LPAD(MONTH(dropOff_datetime), 2, '0'), FLOOR(RAND() * 1000000)) AS DropOffID,
            dropOff_datetime, 
            DOlocationID
        FROM bronze_fhv_record
        GROUP BY dropOff_datetime, DOlocationID
        """
        spark_df = spark.sql(sql_stm)
        spark_df.unpersist()

        return Output(
            spark_df,
            metadata={
                "table": "silver_fhv_dropoff",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    name="silver_fhv_info",
    description="information datetime and Location in fhv taxi trips",
    ins={
        "bronze_fhv_record": AssetIn(
            key_prefix=["bronze", "trip_record"],
        ),
        "silver_fhv_pickup": AssetIn(
            key_prefix=["silver", "trip_record"],
        ),
        "silver_fhv_dropoff": AssetIn(
            key_prefix=["silver", "trip_record"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
)
def silver_fhv_info(
    context,
    bronze_fhv_record: pd.DataFrame,
    silver_fhv_pickup: DataFrame,
    silver_fhv_dropoff: DataFrame,
) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(silver_fhv_info) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {bronze_fhv_record.shape}"
        )

        spark_df = spark.createDataFrame(bronze_fhv_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")
        spark_df.createOrReplaceTempView("bronze_fhv_record")
        silver_fhv_pickup.createOrReplaceTempView("pu")
        silver_fhv_dropoff.createOrReplaceTempView("do")
        sql_stm = """
        SELECT 
            pu.PickUpID,
            do.DropOffID,
            bfr.dispatching_base_num,  
            bfr.Affiliated_base_number
        FROM bronze_fhv_record AS bfr, pu, do
        WHERE pu.PUlocationID = bfr.PUlocationID AND pu.pickup_datetime = bfr.pickup_datetime
        AND do.DOlocationID = bfr.DOlocationID AND do.dropOff_datetime = bfr.dropOff_datetime
        """
        # JOIN bronze_fhv_record fr ON pu.PUlocationID = fr.PUlocationID AND pu.pickup_datetime = fr.pickup_datetime;
        # JOIN bronze_fhv_record fr ON do.DOlocationID = fr.DOlocationID AND do.dropOff_datetime = fr.dropOff_datetime;
        spark_df = spark.sql(sql_stm)
        spark_df.unpersist()
        return Output(
            spark_df,
            metadata={
                "table": "silver_fhv_info",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )
