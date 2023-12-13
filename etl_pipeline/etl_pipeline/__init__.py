from dagster import Definitions
import os
from .assets.bronze_layer import (
    bronze_yellow_record,
    bronze_fhv_record,
    bronze_green_record,
)
from .assets.silver_layer import (
    silver_fhv_pickup,
    silver_fhv_dropoff,
    silver_fhv_info,
    silver_green_record,
)
from .resources.mysql_io_manager import MySQLIOManager
from .resources.minio_io_manager import MinIOIOManager
from .resources.spark_io_manager import SparkIOManager

MYSQL_CONFIG = {
    "host": "de_mysql",
    "port": 3306,
    "database": "trip_record",
    "user": "admin",
    "password": "admin123",
}
MINIO_CONFIG = {
    "endpoint_url": "minio:9000",
    "bucket": "lakehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}
SPARK_CONFIG = {
    "spark_master": os.getenv("SPARK_MASTER_URL"),
    "spark_version": os.getenv("SPARK_VERSION"),
    "hadoop_version": os.getenv("HADOOP_VERSION"),
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

defs = Definitions(
    assets=[
        bronze_yellow_record,
        bronze_fhv_record,
        bronze_green_record,
        silver_fhv_pickup,
        silver_fhv_dropoff,
        silver_fhv_info,
        silver_green_record,
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "spark_io_manager": SparkIOManager(SPARK_CONFIG),
    },
)
