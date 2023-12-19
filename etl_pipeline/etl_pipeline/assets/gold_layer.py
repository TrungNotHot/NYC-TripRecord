import os
from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit
import polars as pl
from ..resources.spark_io_manager import get_spark_session

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
            metadata={"full_load": False, "partition": False},
        ),
        "silver_fhv_pickup": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": True},
        ),
        "bronze_long_lat": AssetIn(
            key_prefix=["bronze", "trip_record"],
            metadata={"full_load": True, "partition": False},
        ),

    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "trip_record"],
    compute_kind="PySpark",
    group_name="gold",
)
def gold_pickup(
    context,
    silver_yellow_pickup: DataFrame,
    silver_green_pickup: DataFrame,
    silver_fhv_pickup: DataFrame,
    bronze_long_lat: pl.DataFrame,
) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_long_lat = bronze_long_lat.to_pandas()
        df_long_lat = spark.createDataFrame(bronze_long_lat)
        df_long_lat.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")

        df_gold_pickup = silver_yellow_pickup.union(silver_green_pickup)
        df_gold_pickup = df_gold_pickup.union(silver_fhv_pickup)

        df_gold_pickup = (
            df_gold_pickup
            .join(df_long_lat, df_gold_pickup.PULocationID == df_long_lat.LocationID , how='left')
        ).drop(df_long_lat.LocationID)

        df_gold_pickup = df_gold_pickup.withColumn("PULocationID", df_gold_pickup["PULocationID"].cast("int")) 


        df_long_lat.unpersist()

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
            metadata={"full_load": False, "partition": False},
        ),
        "silver_fhv_dropoff": AssetIn(
            key_prefix=["silver", "trip_record"],
            metadata={"full_load": True, "partition": True}, 
        ),
        "bronze_long_lat": AssetIn(
            key_prefix=["bronze", "trip_record"],
            metadata={"full_load": True, "partition": False},
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "trip_record"],
    compute_kind="PySpark",
    group_name="gold",
)
def gold_dropoff(
    context,
    silver_yellow_dropoff: DataFrame,
    silver_green_dropoff: DataFrame,
    silver_fhv_dropoff: DataFrame,
    bronze_long_lat: pl.DataFrame,
) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_long_lat = bronze_long_lat.to_pandas()
        df_long_lat = spark.createDataFrame(bronze_long_lat)
        df_long_lat.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")

        df_gold_dropoff = silver_yellow_dropoff.union(silver_green_dropoff)
        df_gold_dropoff = df_gold_dropoff.union(silver_fhv_dropoff)

        df_gold_dropoff = (
            df_gold_dropoff
            .join(df_long_lat, df_gold_dropoff.DOLocationID == df_long_lat.LocationID , how='left')
        )

        df_gold_dropoff = df_gold_dropoff.withColumn("DOLocationID", df_gold_dropoff["DOLocationID"].cast("int"))

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
            metadata={"full_load": False, "partition": False},
        ),

    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "trip_record"],
    compute_kind="PySpark",
    group_name="gold",
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
            metadata={"full_load": False, "partition": False},
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "trip_record"],
    compute_kind="PySpark",
    group_name="gold",
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