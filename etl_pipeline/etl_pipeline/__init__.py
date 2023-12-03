from dagster import Definitions
from assets.bronze_layer import bronze_yellow_record, bronze_fhv_record, bronze_green_record
from resources.mysql_io_manager import MySQLIOManager
from resources.minio_io_manager import MinIOIOManager

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "trip_record",
    "user": "admin",
    "password": "admin123",
}
MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

defs = Definitions(
    assets=[
        bronze_yellow_record,
        bronze_fhv_record,
        bronze_green_record,
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    }
)
