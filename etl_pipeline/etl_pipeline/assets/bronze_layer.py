from dagster import asset, AssetIn, Output, WeeklyPartitionsDefinition
from datetime import datetime
import pandas as pd
WEEKLY = WeeklyPartitionsDefinition(start_date="2023-01-01")


@asset(
    name="bronze_yellow_record",
    description="record of yellow taxi",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "trip_record"],
    compute_kind="MySQL",
    group_name="bronze",
    partitions_def=WEEKLY,
)
def bronze_yellow_record(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM yellow_record;"
    try:
        partition = context.asset_partition_key_for_output()
        partition_by = "tpep_pickup_datetime"
        query += f" WHERE {partition_by} >= {partition} AND {partition_by} < DATE_ADD({partition}, INTERVAL 1 WEEK);"
        context.log.info(f"Partition by {partition_by}: {partition} to 1 week later")
    except Exception:
        context.log.info("No partition key found")
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        df_data,
        metadata={
            "table": "yellow_record",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": str(df_data.columns),
        },
    )


@asset(
    name="bronze_green_record",
    description="record of green taxi",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "trip_record"],
    compute_kind="MySQL",
    group_name="bronze",
)
def bronze_green_record(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM green_record;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        df_data,
        metadata={
            "table": "green_record",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": str(df_data.columns),
        },
    )


@asset(
    name="bronze_fhv_record",
    description="record of fhv taxi",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "trip_record"],
    compute_kind="MySQL",
    group_name="bronze",
)
def bronze_fhv_record(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM fhv_record;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        df_data,
        metadata={
            "table": "fhv_record",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": str(df_data.columns),
        },
    )

