from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
from datetime import datetime
import pandas as pd


@asset(
    name="bronze_yellow_record",
    description="record of yellow taxi",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "trip_record"],
    compute_kind="MySQL",
    group_name="bronze",
)
def bronze_yellow_record(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM yellow_record;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "yellow_record",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
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
        value=df_data,
        metadata={
            "table": "green_record",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
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
        value=df_data,
        metadata={
            "table": "fhv_record",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )

