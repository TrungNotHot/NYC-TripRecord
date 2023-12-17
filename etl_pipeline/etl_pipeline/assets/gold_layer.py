import os
from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
import polars as pl
from pyspark.sql.dataframe import DataFrame
from ..resources.spark_io_manager import get_spark_session
from pyspark.sql.functions import lit
from datetime import datetime, timedelta


def generate_weekly_dates(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    current_date = start_date
    while current_date < end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += timedelta(weeks=1)
def generate_3days_dates(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    current_date = start_date
    while current_date < end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += timedelta(days=3)
start_date_str = "2023-01-01"
end_date_str = "2023-07-01"
three_days = list(generate_3days_dates(start_date_str, end_date_str))
weekly_dates = list(generate_weekly_dates(start_date_str, end_date_str))
WEEKLY = StaticPartitionsDefinition(weekly_dates)
THREE_DAYS = StaticPartitionsDefinition(three_days)


@asset(
    name="gold_pickup",
    description="gold pickup ",
    ins={
        "silver_yellow_pickup": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": True},
        ),
        "silver_green_pickup": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": False},
        ),
        "silver_fhv_pickup": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": True},
        ),

    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "trip_record"],
    compute_kind="PySpark",
    group_name="gold",
    partitions_def=THREE_DAYS,
)
def gold_pickup(
    context,
    silver_yellow_pickup: DataFrame,
    silver_green_pickup: DataFrame,
    silver_fhv_pickup: DataFrame,
) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(gold_pickup) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:

        context.log.info("Got Spark DataFrame, now transforming ...")

        df_gold_pickup = silver_yellow_pickup.union(silver_green_pickup)
        df_gold_pickup = df_gold_pickup.union(silver_fhv_pickup)

        return Output(
            df_gold_pickup,
            metadata={
                "table": "gold_pickup",
                "row_count": df_gold_pickup.count(),
                "column_count": len(df_gold_pickup.columns),
                "columns": df_gold_pickup.columns,
            },
        )


@asset(
    name="gold_dropoff",
    description="gold dropoff ",
    ins={
        "silver_yellow_dropoff": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": True},
        ),
        "silver_green_dropoff": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": False},
        ),
        "silver_fhv_dropoff": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": True}, 
        ),

    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "trip_record"],
    compute_kind="PySpark",
    group_name="gold",
    partitions_def=THREE_DAYS,
)
def gold_dropoff(
    context,
    silver_yellow_dropoff: DataFrame,
    silver_green_dropoff: DataFrame,
    silver_fhv_dropoff: DataFrame,
) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(gold_dropoff) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:

        context.log.info("Got Spark DataFrame, now transforming ...")

        df_gold_dropoff = silver_yellow_dropoff.union(silver_green_dropoff)
        df_gold_dropoff = df_gold_dropoff.union(silver_fhv_dropoff)

        return Output(
            df_gold_dropoff,
            metadata={
                "table": "gold_dropoff",
                "row_count": df_gold_dropoff.count(),
                "column_count": len(df_gold_dropoff.columns),
                "columns": df_gold_dropoff.columns,
            },
        )


@asset(
    name="gold_payment",
    description="gold payment ",
    ins={
        "silver_yellow_payment": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": True},
        ),
        "silver_green_payment": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": False},
        ),

    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "trip_record"],
    compute_kind="PySpark",
    group_name="gold",
    partitions_def=THREE_DAYS,
)
def gold_payment(
    context,
    silver_yellow_payment: DataFrame,
    silver_green_payment: DataFrame,
) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(gold_payment) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:

        context.log.info("Got Spark DataFrame, now transforming ...")

        silver_yellow_payment = silver_yellow_payment.withColumn("airport_fee", lit(""))
        silver_green_payment = silver_green_payment.withColumn("ehail_fee", lit(""))

        df_gold_payment = silver_yellow_payment.union(silver_green_payment)
        
        df_gold_payment = df_gold_payment.withColumn("airport_fee", df_gold_payment["airport_fee"].cast("double"))

        return Output(
            df_gold_payment,
            metadata={
                "table": "gold_payment",
                "row_count": df_gold_payment.count(),
                "column_count": len(df_gold_payment.columns),
                "columns": df_gold_payment.columns,
            },
        )


@asset(
    name="gold_info",
    description="gold_info ",
    ins={
        "silver_yellow_tripinfo": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": True},
        ),
        "silver_green_tripinfo": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": False},
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "trip_record"],
    compute_kind="PySpark",
    group_name="gold",
    partitions_def=THREE_DAYS,
)
def gold_info(
    context,
    silver_yellow_tripinfo: DataFrame,
    silver_green_tripinfo: DataFrame,
) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(gold_info) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:

        context.log.info("Got Spark DataFrame, now transforming ...")
        # transform

        silver_yellow_tripinfo = silver_yellow_tripinfo.withColumn("trip_type", lit(""))

        df_gold_info = silver_yellow_tripinfo.union(silver_green_tripinfo)
        
        df_gold_info = df_gold_info.withColumn("trip_type", df_gold_info["trip_type"].cast("double"))

        return Output(
            df_gold_info,
            metadata={
                "table": "gold_info",
                "row_count": df_gold_info.count(),
                "column_count": len(df_gold_info.columns),
                "columns": df_gold_info.columns,
            },
        )


@asset(
    name="gold_fhv_info",
    description="gold_fhv_info",
    ins={
        "silver_fhv_info": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": True}, 
        ),

    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "trip_record"],
    compute_kind="PySpark",
    group_name="gold",
    partitions_def=THREE_DAYS,
)
def gold_fhv_info(
    context,
    silver_fhv_info: DataFrame,
) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(gold_fhv_info) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:

        context.log.info("Got Spark DataFrame, now transforming ...")

        df_gold_fhv_info = silver_fhv_info

        return Output(
            df_gold_fhv_info,
            metadata={
                "table": "gold_fhv_info",
                "row_count": df_gold_fhv_info.count(),
                "column_count": len(df_gold_fhv_info.columns),
                "columns": df_gold_fhv_info.columns,
            },
        )