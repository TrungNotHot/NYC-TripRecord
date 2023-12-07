from dagster import IOManager, OutputContext, InputContext
from pyspark.sql import SparkSession, DataFrame
from contextlib import contextmanager
from datetime import datetime


@contextmanager
def get_spark_session(config, run_id="Spark IO Manager"):
    executor_memory = "1g" if run_id != "Spark IO Manager" else "1500m"
    try:
        spark = (
            SparkSession.builder.master("local[*]")
            .appName(run_id)
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", executor_memory)
            .config("spark.cores.max", "2")
            .config("spark.executor.cores", "2")
            .config(
                "spark.jars",
                "/usr/local/spark/jars/delta-core_2.12-2.2.0.jar,/usr/local/spark/jars/hadoop-aws-3.3.2.jar,/usr/local/spark/jars/delta-storage-2.2.0.jar,/usr/local/spark/jars/aws-java-sdk-1.12.367.jar,/usr/local/spark/jars/s3-2.18.41.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{config['endpoint_url']}")
            .config("spark.hadoop.fs.s3a.access.key", str(config["minio_access_key"]))
            .config("spark.hadoop.fs.s3a.secret.key", str(config["minio_secret_key"]))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            .getOrCreate()
        )
        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")
class SparkIOManager(IOManager):
    def __init__(self, config):
        self.config = config
    
    def handle_output(self, context: OutputContext, obj: DataFrame):
        # Write output to s3a (MinIO)
        context.log.debug("(Spark handle_output) Writing output to MinIO ...")

        file_path = "s3a://lakehouse/" + "/".join(context.asset_key.path)
        if context.has_partition_key:
            file_path += f"/{context.partition_key}"
        file_path += ".parquet"
        file_name = f"{context.asset_key.path[-1]}"
        
        try:
            obj.write.mode("overwrite").parquet(file_path)
            context.log.debug(f"Saved {file_name} to {file_path}")
        except Exception as e:
            raise Exception(f"(Spark handle_output) Error while writing output: {e}")