import os
from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
import polars as pl
from pyspark.sql.dataframe import DataFrame
from ..resources.spark_io_manager import get_spark_session
from pyspark.sql.functions import monotonically_increasing_id, lit, concat
from datetime import datetime, timedelta

def generate_weekly_dates(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    current_date = start_date
    while current_date < end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += timedelta(weeks=1)

start_date_str = "2023-01-01"
end_date_str = "2023-02-01"
weekly_dates = list(generate_weekly_dates(start_date_str, end_date_str))
WEEKLY = StaticPartitionsDefinition(weekly_dates)



# @asset(
#     name="test_asset",
#     description="pick up datetime and Location in green taxi trips",
#     ins={
#         "bronze_green_record": AssetIn(
#             key_prefix=["bronze", "trip_record"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "trip_record"],
#     compute_kind="PySpark",
#     group_name="silver",
# )
# def test_asset(
#     context, bronze_green_record
# ) -> Output[DataFrame]:
#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     context.log.debug("(silver_green_pickup) Creating spark session ...")

#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         context.log.debug(
#             f"Converted to pandas DataFrame with shape: {bronze_green_record.shape}"
#         )

#         spark_df = spark.createDataFrame(bronze_green_record)
#         spark_df.cache()
#         context.log.info("Got Spark DataFrame")
#         # transform
#         spark_df.unpersist()

#         return Output(
#             spark_df,
#             metadata={
#                 "table": "test_asset",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#             },
#         )

# _______________________________________FHV assets_______________________________________________________
@asset(
    name="silver_fhv_pickup",
    description="pick up datetime and location in fhv taxi trips",
    ins={
        "bronze_fhv_record": AssetIn(
            key_prefix=["bronze", "trip_record"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
    partitions_def=WEEKLY,
)
def silver_fhv_pickup(context, bronze_fhv_record: pl.DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_fhv_record = bronze_fhv_record.to_pandas()
        spark_df = spark.createDataFrame(bronze_fhv_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        # transform
        select_cols = ["pickup_datetime", "PUlocationID"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        specialID = concat(lit(f"F{''.join(context.partition_key.split('-'))}"), monotonically_increasing_id())
        spark_df = spark_df.withColumn("PickUpID", specialID)
        spark_df = spark_df.withColumnRenamed('PUlocationID','PULocationID')

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
    description="drop off datetime and location in fhv taxi trips",
    ins={
        "bronze_fhv_record": AssetIn(
            key_prefix=["bronze", "trip_record"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
    partitions_def=WEEKLY,
)
def silver_fhv_dropoff(context, bronze_fhv_record: pl.DataFrame) -> Output[DataFrame]:
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("(silver_fhv_dropoff) Creating spark session ...")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_fhv_record =   bronze_fhv_record.to_pandas()
        spark_df = spark.createDataFrame(bronze_fhv_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        # transform
        select_cols = ["dropoff_datetime", "DOlocationID"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        specialID = concat(lit(f"F{''.join(context.partition_key.split('-'))}"), monotonically_increasing_id())
        spark_df = spark_df.withColumn("DropOffID", specialID)
        spark_df = spark_df.withColumnRenamed('DOlocationID','DOLocationID')

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
    description="trip information in fhv taxi trips",
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
    partitions_def=WEEKLY,
)
def silver_fhv_info(
    context,
    bronze_fhv_record: pl.DataFrame,
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
        bronze_fhv_record =   bronze_fhv_record.to_pandas()
        df_bronze_fhv_record = spark.createDataFrame(bronze_fhv_record)
        df_bronze_fhv_record.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        # transform
        df_bronze_fhv_record = df_bronze_fhv_record.withColumnRenamed('PUlocationID','PULocationID') 
        df_bronze_fhv_record = df_bronze_fhv_record.withColumnRenamed('DOlocationID','DOLocationID')
        df_bronze_fhv_record = df_bronze_fhv_record.withColumnRenamed('Affiliated_base_number','affiliated_base_number')
        df_bronze_fhv_record = df_bronze_fhv_record.withColumnRenamed('SR_Flag','sr_flag')

        select_cols_pickup = ["pickup_datetime", "PULocationID"]
        select_cols_dropoff = ["dropoff_datetime", "DOLocationID"]

        df_bronze_fhv_record = (
            df_bronze_fhv_record
            .join(silver_fhv_pickup, on = select_cols_pickup, how='left')
            .join(silver_fhv_dropoff, on = select_cols_dropoff, how='left')
        )

        df_bronze_fhv_record =df_bronze_fhv_record.select([
            'PickUpID', 'DropOffID', 
            'dispatching_base_num', 'affiliated_base_number', 'sr_flag'
        ])
        df_bronze_fhv_record = df_bronze_fhv_record.na.drop(subset=['dispatching_base_num'])
        df_bronze_fhv_record = df_bronze_fhv_record.na.drop(subset=['affiliated_base_number'])

        df_bronze_fhv_record.unpersist()

        return Output(
            df_bronze_fhv_record,
            metadata={
                "table": "silver_fhv_info",
                "row_count": df_bronze_fhv_record.count(),
                "column_count": len(df_bronze_fhv_record.columns),
                "columns": df_bronze_fhv_record.columns,
            },
        )


#___________________________________ Yellow assets_____________________________________________
@asset(
    name="silver_yellow_pickup",
    description="pick up datetime and location in yellow taxi trips",
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
def silver_yellow_pickup(context, bronze_yellow_record: pl.DataFrame) -> Output[DataFrame]:

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_yellow_record = bronze_yellow_record.to_pandas()
        spark_df = spark.createDataFrame(bronze_yellow_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        # transform
        select_cols = ["tpep_pickup_datetime", "PULocationID"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        specialID = concat(lit(f"Y{''.join(context.partition_key.split('-'))}"), monotonically_increasing_id())
        spark_df = spark_df.withColumn("PickUpID", specialID)
        spark_df = spark_df.withColumnRenamed('tpep_pickup_datetime','pickup_datetime')
        spark_df = spark_df.na.drop(subset=["pickup_datetime"])

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
    description="drop off datetime and location in yellow taxi trips",
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
def silver_yellow_dropoff(context, bronze_yellow_record: pl.DataFrame) -> Output[DataFrame]:

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_yellow_record = bronze_yellow_record.to_pandas()
        spark_df = spark.createDataFrame(bronze_yellow_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        # transform
        select_cols = ["tpep_dropoff_datetime", "DOLocationID"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        specialID = concat(lit(f"Y{''.join(context.partition_key.split('-'))}"), monotonically_increasing_id())
        spark_df = spark_df.withColumn("DropOffID", specialID)
        spark_df = spark_df.withColumnRenamed('tpep_dropoff_datetime','Dropoff_datetime')
        spark_df = spark_df.na.drop(subset=["dropoff_datetime"])

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
    description="Payment information in yellow taxi trips",
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
def silver_yellow_payment(context, bronze_yellow_record: pl.DataFrame) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_yellow_record = bronze_yellow_record.to_pandas()
        spark_df = spark.createDataFrame(bronze_yellow_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        # transform
        select_cols = ["fare_amount", "mta_tax", "improvement_surcharge", "payment_type", "RatecodeID", "extra", "tip_amount", "tolls_amount","total_amount","congestion_surcharge", "airport_fee"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        specialID = concat(lit(f"Y{''.join(context.partition_key.split('-'))}"), monotonically_increasing_id())
        spark_df = spark_df.withColumn("PaymentID", specialID)
        spark_df = spark_df.na.drop(subset=["fare_amount"])
        spark_df = spark_df.na.drop(subset=["mta_tax"])
        spark_df = spark_df.na.drop(subset=["improvement_surcharge"])
        spark_df = spark_df.na.drop(subset=["payment_type"])
        spark_df = spark_df.na.drop(subset=["RatecodeID"])
        spark_df = spark_df.na.drop(subset=["extra"])
        spark_df = spark_df.na.drop(subset=["tip_amount"])
        spark_df = spark_df.na.drop(subset=["tolls_amount"])
        spark_df = spark_df.na.drop(subset=["total_amount"])
        spark_df = spark_df.na.drop(subset=["congestion_surcharge"])

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
    description="tripinfo in yellow taxi trips",
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
    bronze_yellow_record: pl.DataFrame,
    silver_yellow_pickup: DataFrame,
    silver_yellow_dropoff: DataFrame,
    silver_yellow_payment: DataFrame,
    ) -> Output[DataFrame]:

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_yellow_record = bronze_yellow_record.to_pandas()
        df_bronze_yellow_record = spark.createDataFrame(bronze_yellow_record)
        df_bronze_yellow_record.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        # transform
        df_bronze_yellow_record = df_bronze_yellow_record.withColumnRenamed('tpep_pickup_datetime','pickup_datetime') 
        df_bronze_yellow_record = df_bronze_yellow_record.withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')

        select_cols_pickup = ["pickup_datetime", "PULocationID"]
        select_cols_dropoff = ["dropoff_datetime", "DOLocationID"]
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
        spark_df = spark_df.na.drop(subset=['VendorID'])
        spark_df = spark_df.na.drop(subset=['passenger_count'])
        spark_df = spark_df.na.drop(subset=['trip_distance'])
        spark_df = spark_df.na.drop(subset=['store_and_fwd_flag'])

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
# ______________________________________________Green assets_______________________________________________________
@asset(
    name="silver_green_pickup",
    description="pick up datetime and location in green taxi trips",
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
def silver_green_pickup(context, bronze_green_record: pl.DataFrame) -> Output[DataFrame]:

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_green_record = bronze_green_record.to_pandas()
        spark_df = spark.createDataFrame(bronze_green_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        spark_df = spark_df.withColumnRenamed('lpep_pickup_datetime','pickup_datetime')
        # transform
        select_cols = ["pickup_datetime", "PULocationID"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        specialID = monotonically_increasing_id()
        spark_df = spark_df.withColumn("PickUpID", specialID)
        spark_df = spark_df.na.drop(subset=["pickup_datetime"])

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
    

@asset(
    name="silver_green_dropoff",
    description="Drop off datetime and location in green taxi trips",
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
def silver_green_dropoff(context, bronze_green_record: pl.DataFrame) -> Output[DataFrame]:

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_green_record = bronze_green_record.to_pandas()
        spark_df = spark.createDataFrame(bronze_green_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        spark_df = spark_df.withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')
        # transform
        select_cols = ["dropoff_datetime", "DOLocationID"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        specialID = monotonically_increasing_id()
        spark_df = spark_df.withColumn("DropOffID", specialID)
        spark_df = spark_df.na.drop(subset=["dropoff_datetime"])

        spark_df.unpersist()
        
        return Output(
            spark_df,
            metadata={
                "table": "silver_green_dropoff",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )
    

@asset(
    name="silver_green_payment",
    description="Payment information in green taxi trips",
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
def silver_green_payment(context, bronze_green_record: pl.DataFrame) -> Output[DataFrame]:
    
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_green_record = bronze_green_record.to_pandas()
        spark_df = spark.createDataFrame(bronze_green_record)
        spark_df.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        # transform
        select_cols = ["fare_amount", "mta_tax", "improvement_surcharge", "payment_type", "RatecodeID", "extra", "tip_amount", "tolls_amount","total_amount","congestion_surcharge", "ehail_fee"]
        spark_df = spark_df.select(select_cols)
        spark_df = spark_df.dropDuplicates(select_cols)
        specialID = monotonically_increasing_id()
        spark_df = spark_df.withColumn("PaymentID", specialID)
        spark_df = spark_df.na.drop(subset=["fare_amount"])
        spark_df = spark_df.na.drop(subset=["mta_tax"])
        spark_df = spark_df.na.drop(subset=["improvement_surcharge"])
        spark_df = spark_df.na.drop(subset=["payment_type"])
        spark_df = spark_df.na.drop(subset=["RatecodeID"])
        spark_df = spark_df.na.drop(subset=["extra"])
        spark_df = spark_df.na.drop(subset=["tip_amount"])
        spark_df = spark_df.na.drop(subset=["tolls_amount"])
        spark_df = spark_df.na.drop(subset=["total_amount"])
        spark_df = spark_df.na.drop(subset=["congestion_surcharge"])


        spark_df.unpersist()
        
        return Output(
            spark_df,
            metadata={
                "table": "silver_green_payment",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    name="silver_green_tripinfo",
    description="tripinfo in green taxi trips",
    ins={
        "bronze_green_record": AssetIn(key_prefix=["bronze", "trip_record"]),
        "silver_green_pickup": AssetIn(key_prefix=["silver", "trip_record"]),
        "silver_green_dropoff": AssetIn(key_prefix=["silver", "trip_record"]),
        "silver_green_payment": AssetIn(key_prefix=["silver", "trip_record"]),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "trip_record"],
    compute_kind="PySpark",
    group_name="silver",
)
def silver_green_tripinfo(
    context, 
    bronze_green_record: pl.DataFrame,
    silver_green_pickup: DataFrame,
    silver_green_dropoff: DataFrame,
    silver_green_payment: DataFrame,
    ) -> Output[DataFrame]:

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        bronze_green_record = bronze_green_record.to_pandas()
        df_bronze_green_record = spark.createDataFrame(bronze_green_record)
        df_bronze_green_record.cache()
        context.log.info("Got Spark DataFrame, now transforming ...")
        # transform
        df_bronze_green_record = df_bronze_green_record.withColumnRenamed('lpep_pickup_datetime','pickup_datetime') 
        df_bronze_green_record = df_bronze_green_record.withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')

        select_cols_pickup = ["pickup_datetime", "PULocationID"]
        select_cols_dropoff = ["dropoff_datetime", "DOLocationID"]
        select_cols_payment = ["fare_amount", "mta_tax", "improvement_surcharge", "payment_type", "RatecodeID", "extra", "tip_amount", "tolls_amount","total_amount","congestion_surcharge", "ehail_fee"]
        spark_df = (
            df_bronze_green_record
            .join(silver_green_pickup, on = select_cols_pickup, how='left')
            .join(silver_green_dropoff, on = select_cols_dropoff, how='left')
            .join(silver_green_payment, on = select_cols_payment, how='left')
        )

        spark_df =spark_df.select([
            'VendorID', 'PickUpID', 'DropOffID', 
            'PaymentID', 'passenger_count', 'trip_distance', 'store_and_fwd_flag', 'trip_type'
        ])
        spark_df = spark_df.na.drop(subset=['VendorID'])
        spark_df = spark_df.na.drop(subset=['passenger_count'])
        spark_df = spark_df.na.drop(subset=['trip_distance'])
        spark_df = spark_df.na.drop(subset=['store_and_fwd_flag'])
        spark_df = spark_df.na.drop(subset=['trip_type'])

        df_bronze_green_record.unpersist()

        return Output(
            spark_df,
            metadata={
                "table": "silver_green_tripinfo",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )