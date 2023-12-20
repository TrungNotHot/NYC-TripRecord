from dagster import asset, AssetIn, Output
from pyspark.sql import DataFrame


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
        "columns": ["PickUpID", "PULocationID", "pickup_datetime", "longitude", "latitude"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse"],  
    compute_kind="Postgres",
    group_name="warehouse",
)
def warehouse_pickup(context, gold_pickup: DataFrame):

    context.log.info("Got spark DataFrame, loading to postgres")

    return Output(
        gold_pickup,
        metadata={
            "database": "trip_record",
            "schema": "warehouse",
            "table": "warehouse_pickup",
            "primary_keys": ["PickUpID"],
            "row_count": gold_pickup.count(),
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
        "columns": ["DropOffID", "DOLocationID", "Dropoff_datetime", "longitude", "latitude"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse"],  
    compute_kind="Postgres",
    group_name="warehouse",
)
def warehouse_dropoff(context, gold_dropoff: DataFrame):

    context.log.info("Got spark DataFrame, loading to postgres")

    return Output(
        gold_dropoff,
        metadata={
            "database": "trip_record",
            "schema": "warehouse",
            "table": "warehouse_dropoff",
            "primary_keys": ["DropOffID"],
            "row_count": gold_dropoff.count(),
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
    key_prefix=["warehouse"],  
    compute_kind="Postgres",
    group_name="warehouse",
)
def warehouse_payment(context, gold_payment: DataFrame):

    context.log.info("Got spark DataFrame, loading to postgres")

    return Output(
        gold_payment,
        metadata={
            "database": "trip_record",
            "schema": "warehouse",
            "table": "warehouse_payment",
            "primary_keys": ["PaymentID"],
            "row_count": gold_payment.count(),
        },
    )


@asset(
    name = 'warehouse_info',
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
    key_prefix=["warehouse"],  
    compute_kind="Postgres",
    group_name="warehouse",
)
def warehouse_info(context, gold_info: DataFrame):

    context.log.info("Got spark DataFrame, loading to postgres")

    return Output(
        gold_info,
        metadata={
            "database": "trip_record",
            "schema": "warehouse",
            "table": "warehouse_tripinfo",
            "primary_keys": ["PickUpID", "DropOffID", "PaymentID"],
            "row_count": gold_info.count(),
        },
    )




@asset(
    name = 'warehouse_fhv_info',
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
    key_prefix=["warehouse"],  
    compute_kind="Postgres",
    group_name="warehouse",
)
def warehouse_fhv_info(context, gold_fhv_info: DataFrame):

    context.log.info("Got spark DataFrame, loading to postgres")

    return Output(
        gold_fhv_info,
        metadata={
            "database": "trip_record",
            "schema": "warehouse",
            "table": "warehouse_fhv_info",
            "primary_keys": ["PickUpID", "DropOffID"],
            "row_count": gold_fhv_info.count(),
        },
    )

