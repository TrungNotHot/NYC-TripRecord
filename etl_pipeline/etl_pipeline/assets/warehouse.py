import os
from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
import polars as pl
import pyarrow as pa
from pyspark.sql import DataFrame
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
end_date_str = "2023-04-01"
three_days = list(generate_3days_dates(start_date_str, end_date_str))
weekly_dates = list(generate_weekly_dates(start_date_str, end_date_str))
WEEKLY = StaticPartitionsDefinition(weekly_dates)
THREE_DAYS = StaticPartitionsDefinition(three_days)


@asset(
    name = 'warehouse_pickup',
    description="Load gold_pickup data from spark to postgres",
    ins={
        "gold_pickup": AssetIn(
            key_prefix=["gold", "trip_record"],
        ),
    },
    metadata={
        "primary_keys": ["PickUpID"],
        "columns": ["PickUpID", "PULocationID", "pickup_datetime"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["gold"],  
    compute_kind="Postgres",
    group_name="warehouse",
)
def warehouse_pickup(context, gold_pickup: DataFrame):


    context.log.info("Got spark DataFrame, loading to postgres")

    df = pl.from_arrow(pa.Table.from_batches(gold_pickup._collect_as_arrow()))
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=df,
        metadata={
            "database": "warehouse_pickup",
            "schema": "gold",
            "table": "warehouse_pickup",
            "primary_keys": ["PickUpID"],
            "columns": ["PickUpID", "PULocationID", "pickup_datetime"],
        },
    )


@asset(
    name = 'warehouse_dropoff',
    description="Load gold_pickup data from spark to postgres",
    ins={
        "gold_dropoff": AssetIn(
            key_prefix=["gold", "trip_record"],
        ),
    },
    metadata={
        "primary_keys": ["DropOffID"],
        "columns": ["DropOffID", "DOLocationID", "Dropoff_datetime"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["gold"],  
    compute_kind="Postgres",
    group_name="warehouse",
)
def warehouse_dropoff(context, gold_dropoff: DataFrame):


    context.log.info("Got spark DataFrame, loading to postgres")

    df = pl.from_arrow(pa.Table.from_batches(gold_dropoff._collect_as_arrow()))
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=df,
        metadata={
            "database": "warehouse_dropoff",
            "schema": "gold",
            "table": "warehouse_dropoff",
            "primary_keys": ["DropOffID"],
            "columns": ["DropOffID", "DOLocationID", "Dropoff_datetime"],
        },
    )


@asset(
    name = 'warehouse_payment',
    description="Load gold_pickup data from spark to postgres",
    ins={
        "gold_payment": AssetIn(
            key_prefix=["gold", "trip_record"],
        ),
    },
    metadata={
        "primary_keys": ["PaymentID"],
        "columns": ["PaymentID", "fare_amount", "mta_tax", "improvement_surcharge", "payment_type", "RatecodeID", "extra", "tip_amount", "tolls_amount", "total_amount", "congestion_surcharge", "airport_fee"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["gold"],  
    compute_kind="Postgres",
    group_name="warehouse",
)
def warehouse_payment(context, gold_payment: DataFrame):


    context.log.info("Got spark DataFrame, loading to postgres")

    df = pl.from_arrow(pa.Table.from_batches(gold_payment._collect_as_arrow()))
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=df,
        metadata={
            "database": "warehouse_payment",
            "schema": "gold",
            "table": "warehouse_payment",
            "primary_keys": ["PaymentID"],
            "columns": ["PaymentID", "fare_amount", "mta_tax", "improvement_surcharge", "payment_type", "RatecodeID", "extra", "tip_amount", "tolls_amount", "total_amount", "congestion_surcharge", "airport_fee"],
        },
    )




@asset(
    name = 'warehouse_tripinfo',
    description="Load gold_info data from spark to postgres",
    ins={
        "gold_info": AssetIn(
            key_prefix=["gold", "trip_record"],
        ),
    },
    metadata={
        "primary_keys": ["PickUpID", "DropOffID", "PaymentID"],
        "columns": ["PickUpID", "DropOffID", "PaymentID", "VendorID", "passenger_count", "trip_distance", "store_and_fwd_flag"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["gold"],  
    compute_kind="Postgres",
    group_name="warehouse",
)
def warehouse_tripinfo(context, gold_info: DataFrame):


    context.log.info("Got spark DataFrame, loading to postgres")

    df = pl.from_arrow(pa.Table.from_batches(gold_info._collect_as_arrow()))
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=df,
        metadata={
            "database": "warehouse_tripinfo",
            "schema": "gold",
            "table": "warehouse_tripinfo",
            "primary_keys": ["PickUpID", "DropOffID", "PaymentID"],
            "columns": ["PickUpID", "DropOffID", "PaymentID", "VendorID", "passenger_count", "trip_distance", "store_and_fwd_flag"],
        },
    )




@asset(
    name = 'warehouse_fhvinfo',
    description="Load gold_fhv_info data from spark to postgres",
    ins={
        "gold_fhv_info": AssetIn(
            key_prefix=["gold", "trip_record"],
        ),
    },
    metadata={
        "primary_keys": ["PickUpID", "DropOffID"],
        "columns": ["PickUpID", "DropOffID", "dispatching_base_num", "affiliated_base_number", "sr_flag"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["gold"],  
    compute_kind="Postgres",
    group_name="warehouse",
)
def warehouse_fhvinfo(context, gold_fhv_info: DataFrame):


    context.log.info("Got spark DataFrame, loading to postgres")

    df = pl.from_arrow(pa.Table.from_batches(gold_fhv_info._collect_as_arrow()))
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=df,
        metadata={
            "database": "warehouse_fhvinfo",
            "schema": "gold",
            "table": "warehouse_fhvinfo",
            "primary_keys": ["PickUpID", "DropOffID"],
            "columns": ["PickUpID", "DropOffID", "dispatching_base_num", "affiliated_base_number", "sr_flag"],
        },
    )

