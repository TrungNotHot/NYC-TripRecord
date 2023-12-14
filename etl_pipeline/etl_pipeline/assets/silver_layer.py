import os
from dagster import asset, AssetIn, Output, WeeklyPartitionsDefinition
import pandas as pd
from pyspark.sql import DataFrame
from ..resources.spark_io_manager import get_spark_session
from pyspark.sql.functions import monotonically_increasing_id, lit

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
def silver_green_record(context, bronze_green_record: pd.DataFrame) -> Output[DataFrame]:
    
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
    name="silver_yellow_pickup",
    description="pick up datetime and Location in yellow taxi trips",
    ins={
        "bronze_yellow_record": AssetIn(
            key_prefix=["bronze", "trip_record"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
    partitions_def=WEEKLY,
)

def silver_yellow_pickup(context, bronze_yellow_record: pd.DataFrame) -> Output[DataFrame]:

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(silver_yellow_pickup) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        spark_df = spark.createDataFrame(bronze_yellow_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")
        # transform
        select_cols = ["tpep_pickup_datetime", "PULocationID"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        spark_df = spark_df.withColumn("PickUpID", lit(f"F{''.join(context.partition_key.split('-'))}") + monotonically_increasing_id())
        spark_df = spark_df.withColumnRenamed('tpep_pickup_datetime','Pickup_datetime')
        spark_df.unpersist()
        return Output(
            spark_df,
            metadata={
                "table": "silver_yellow_pickup",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    name="silver_yellow_dropoff",
    description="drop off datetime and Location in yellow taxi trips",
    partitions_def=WEEKLY,
    ins={
        "bronze_yellow_record": AssetIn(
            key_prefix=["bronze", "trip_record"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
)

def silver_yellow_dropoff(context, bronze_yellow_record: pd.DataFrame) -> Output[DataFrame]:

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(silver_yellow_dropoff) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        spark_df = spark.createDataFrame(bronze_yellow_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")
        # transform
        select_cols = ["tpep_dropoff_datetime", "DOLocationID"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        spark_df = spark_df.withColumn("DropOffID", monotonically_increasing_id())  
        spark_df = spark_df.withColumnRenamed('tpep_dropoff_datetime','Dropoff_datetime')

        spark_df.unpersist()
        return Output(
            spark_df,
            metadata={
                "table": "silver_yellow_dropoff",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )

@asset(
    name="silver_yellow_payment",
    description="Payment",
    partitions_def=WEEKLY,
    ins={
        "bronze_yellow_record": AssetIn(
            key_prefix=["bronze", "trip_record"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
)

def silver_yellow_payment(context, bronze_yellow_record: pd.DataFrame) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(silver_yellow_payment) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        spark_df = spark.createDataFrame(bronze_yellow_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")
        # transform
        select_cols = ["fare_amount", "mta_tax", "improvement_surcharge", "payment_type", "RatecodeID", "extra", "tip_amount", "tolls_amount","total_amount","congestion_surcharge", "airport_fee"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        spark_df = spark_df.withColumn("PaymentID", monotonically_increasing_id())  
            
        spark_df.unpersist()
        
        return Output(
            spark_df,
            metadata={
                "table": "silver_yellow_payment",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    name="silver_yellow_tripinfo",
    description="info",
    partitions_def=WEEKLY,
    ins={
        "bronze_yellow_record": AssetIn(key_prefix=["bronze", "trip_record"]),
        "silver_yellow_pickup": AssetIn(key_prefix=["silver", "trip_record"]),
        "silver_yellow_dropoff": AssetIn(key_prefix=["silver", "trip_record"]),
        "silver_yellow_payment": AssetIn(key_prefix=["silver", "trip_record"]),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
)


def silver_yellow_tripinfo(
    context, 
    bronze_yellow_record: pd.DataFrame,
    silver_yellow_pickup: DataFrame,
    silver_yellow_dropoff: DataFrame,
    silver_yellow_payment: DataFrame,
    ) -> Output[DataFrame]:

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(silver_yellow_payment) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
    
        df_bronze_yellow_record = spark.createDataFrame(bronze_yellow_record)
        df_bronze_yellow_record.cache()

        df_bronze_yellow_record = df_bronze_yellow_record.withColumnRenamed('tpep_pickup_datetime','Pickup_datetime') 
        df_bronze_yellow_record = df_bronze_yellow_record.withColumnRenamed('tpep_dropoff_datetime','Dropoff_datetime')

        select_cols_pickup = ["Pickup_datetime", "PULocationID"]
        select_cols_dropoff = ["Dropoff_datetime", "DOLocationID"]
        select_cols_payment = ["fare_amount", "mta_tax", "improvement_surcharge", "payment_type", "RatecodeID", "extra", "tip_amount", "tolls_amount","total_amount","congestion_surcharge", "airport_fee"]
        spark_df = (
            df_bronze_yellow_record
            .join(silver_yellow_pickup, on = select_cols_pickup, how='left')
            .join(silver_yellow_dropoff, on = select_cols_dropoff, how='left')
            .join(silver_yellow_payment, on = select_cols_payment, how='left')
        )

        spark_df =spark_df.select([
            'VendorID', 'PickUpID', 'DropOffID', 
            'PaymentID', 'passenger_count', 'trip_distance', 'store_and_fwd_flag'
        ])

        df_bronze_yellow_record.unpersist()

        return Output(
            spark_df,
            metadata={
                "table": "silver_yellow_tripinfo",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )